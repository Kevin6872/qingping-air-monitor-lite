"""Control a HomeKit-mode CGDN1 through the Qingping App cloud channel."""
from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import timedelta
import hashlib
import json
import logging
import secrets
import time
from typing import Any

from aiohttp import ClientError, ClientWebSocketResponse, WSMsgType

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_MAC, CONF_MODEL
from homeassistant.core import HomeAssistant, callback
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator
from homeassistant.util import dt as dt_util

from .const import (
    CONF_QINGPING_APP_SECRET,
    CONF_QINGPING_ACCESS_TOKEN,
    CONF_QINGPING_USER_ID,
    DOMAIN,
)
from .homekit_source import QingpingHomeKitSource

_LOGGER = logging.getLogger(__name__)

API_HOST = "https://qingplus.cleargrass.com"
APP_ID = "com.cleargrass.app.Air"
APP_VERSION = "4.0.1"
APP_BUILD = "202604241635"

CLOUD_AVAILABLE = "qingping_cloud_available"
CLOUD_CONNECTED = "qingping_cloud_connected"
CLOUD_LAST_MESSAGE = "qingping_cloud_last_message"
CLOUD_SETTINGS = "qingping_cloud_settings"
CLOUD_ERROR = "qingping_cloud_error"

_DEVICE_RESPONSE_TIMEOUT = 15
_DEVICE_AVAILABLE_TIMEOUT = 180
_QUERY_INTERVAL = 90


def _encode_utf8(value: str) -> bytes:
    body = value.encode()
    return len(body).to_bytes(2, "big") + body


def _encode_remaining_length(value: int) -> bytes:
    result = bytearray()
    while True:
        byte = value % 128
        value //= 128
        if value:
            byte |= 0x80
        result.append(byte)
        if not value:
            return bytes(result)


def _mqtt_packet(header: int, body: bytes) -> bytes:
    return bytes([header]) + _encode_remaining_length(len(body)) + body


def _mqtt_connect_packet(
    client_id: str, username: str, password: str, keepalive: int = 60
) -> bytes:
    variable_header = _encode_utf8("MQTT") + bytes(
        [4, 0xC2, keepalive >> 8, keepalive & 0xFF]
    )
    payload = (
        _encode_utf8(client_id) + _encode_utf8(username) + _encode_utf8(password)
    )
    return _mqtt_packet(0x10, variable_header + payload)


def _mqtt_subscribe_packet(topic: str, packet_id: int = 1) -> bytes:
    body = packet_id.to_bytes(2, "big") + _encode_utf8(topic) + b"\x00"
    return _mqtt_packet(0x82, body)


def _mqtt_publish_packet(topic: str, payload: str) -> bytes:
    return _mqtt_packet(0x30, _encode_utf8(topic) + payload.encode())


def _decode_remaining_length(packet: bytes, offset: int = 1) -> tuple[int, int]:
    value = 0
    multiplier = 1
    while True:
        if offset >= len(packet):
            raise ValueError("truncated MQTT remaining length")
        byte = packet[offset]
        offset += 1
        value += (byte & 0x7F) * multiplier
        if not byte & 0x80:
            return value, offset
        multiplier *= 128
        if multiplier > 128**3:
            raise ValueError("invalid MQTT remaining length")


def _decode_publish(packet: bytes) -> tuple[str, str]:
    _remaining, offset = _decode_remaining_length(packet)
    if offset + 2 > len(packet):
        raise ValueError("truncated MQTT publish packet")
    topic_length = int.from_bytes(packet[offset : offset + 2], "big")
    topic_start = offset + 2
    payload_start = topic_start + topic_length
    if payload_start > len(packet):
        raise ValueError("truncated MQTT publish topic")
    return (
        packet[topic_start:payload_start].decode(),
        packet[payload_start:].decode(),
    )


def _unwrap_device_message(
    payload: str,
) -> tuple[str | None, str | None, dict[str, Any]]:
    """Unwrap Qingping's outer MQTT envelope and inner JSON string."""
    outer = json.loads(payload)
    if not isinstance(outer, dict):
        raise ValueError("Qingping MQTT payload is not an object")
    mac = outer.get("mac")
    inner = outer.get("data", outer)
    if isinstance(inner, str):
        inner = json.loads(inner)
    if not isinstance(inner, dict):
        raise ValueError("Qingping device data is not an object")
    cid = outer.get("cid") or inner.get("cid")
    return str(mac) if mac else None, str(cid) if cid else None, inner


@dataclass
class _ResponseWaiter:
    """A request-specific MQTT response waiter."""

    response_type: str
    future: asyncio.Future
    matcher: Callable[[Mapping[str, Any]], bool] | None = None


class QingpingCloudSource:
    """Own the vendor cloud session used by a HomeKit-mode Dany device."""

    def __init__(
        self,
        hass: HomeAssistant,
        config_entry: ConfigEntry,
        coordinator: DataUpdateCoordinator,
        runtime: dict[str, Any],
        homekit_source: QingpingHomeKitSource,
    ) -> None:
        self.hass = hass
        self.config_entry = config_entry
        self.coordinator = coordinator
        self.runtime = runtime
        self.homekit_source = homekit_source
        self.mac = str(config_entry.data.get(CONF_MAC, "")).replace(":", "").upper()

        self._app_secret = str(
            config_entry.data.get(CONF_QINGPING_APP_SECRET, "")
        ).strip()
        self._access_token = config_entry.data.get(CONF_QINGPING_ACCESS_TOKEN)
        self._user_id = config_entry.data.get(CONF_QINGPING_USER_ID)
        self._mqtt: dict[str, Any] = {}
        self._ws: ClientWebSocketResponse | None = None
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self._connected = False
        self._last_message_monotonic: float | None = None
        self._last_message: str | None = None
        self._last_error: str | None = None
        self._settings: dict[str, Any] = {}
        self._response_waiters: dict[str, _ResponseWaiter] = {}
        self._write_lock = asyncio.Lock()

    @property
    def connected(self) -> bool:
        """Return whether the vendor MQTT session is connected."""
        return self._connected

    @property
    def available(self) -> bool:
        """Return whether the physical device recently answered on the cloud bus."""
        return bool(
            self._connected
            and self._last_message_monotonic is not None
            and time.monotonic() - self._last_message_monotonic
            <= _DEVICE_AVAILABLE_TIMEOUT
        )

    @property
    def last_message(self) -> str | None:
        return self._last_message

    @property
    def last_error(self) -> str | None:
        return self._last_error

    @property
    def broker_url(self) -> str | None:
        value = self._mqtt.get("brokerUrl")
        return str(value) if value else None

    @property
    def sub_topic(self) -> str | None:
        value = self._mqtt.get("subTopic")
        return str(value) if value else None

    @property
    def pub_topic(self) -> str | None:
        value = self._mqtt.get("pubTopic")
        return str(value) if value else None

    @property
    def settings(self) -> Mapping[str, Any]:
        return self._settings

    async def async_start(self) -> None:
        """Prepare the App session, then maintain the cloud MQTT connection."""
        if self.config_entry.data.get(CONF_MODEL) != "CGDN1":
            return
        if not self._app_secret:
            self._set_error("Qingping App signing key is missing")
            return
        try:
            await self._async_prepare_session()
        except (ClientError, TimeoutError, HomeAssistantError, ValueError) as err:
            self._set_error(f"Cloud session setup failed: {err}")
            _LOGGER.warning("Qingping cloud setup failed for %s: %s", self.mac, err)

        self._task = self.config_entry.async_create_background_task(
            self.hass,
            self._async_run_forever(),
            f"{DOMAIN}-{self.mac}-cloud",
        )

    async def async_stop(self) -> None:
        """Stop the cloud connection and fail any waiting service call."""
        self._stop.set()
        if self._ws is not None:
            await self._ws.close()
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        self._set_connected(False)
        self._fail_waiters(HomeAssistantError("Qingping cloud connection stopped"))

    async def async_query_settings(self) -> dict[str, Any]:
        """Request type 28 settings and wait for this physical device's reply."""
        response = await self._async_send_and_wait(
            {"type": "28"},
            "28",
            matcher=lambda data: isinstance(data.get("setting"), dict),
        )
        settings = response.get("setting")
        if not isinstance(settings, dict):
            raise HomeAssistantError("Qingping device returned no settings")
        return dict(settings)

    async def async_set_settings(self, changes: Mapping[str, Any]) -> dict[str, Any]:
        """Send App-compatible type 17 settings, then query and verify readback."""
        if not changes:
            return dict(self._settings)

        async with self._write_lock:
            requested = dict(changes)
            # Type 17 requires the complete settings object. Always fetch a
            # fresh snapshot immediately before merging so a recent App change
            # is not overwritten by HA's older cache.
            current = await self.async_query_settings()
            merged = {**current, **requested}
            merged.setdefault("is_12_hour_mode", current.get("is_12_hour_mode", 0))
            merged.setdefault("pm25_standard", current.get("pm25_standard", 0))

            response = await self._async_send_and_wait(
                {"type": "17", "setting": merged},
                "28",
                matcher=lambda data: self._settings_include(data, requested),
            )
            acknowledged = response.get("setting")
            if not isinstance(acknowledged, dict):
                raise HomeAssistantError(
                    "Qingping device did not return its settings acknowledgement"
                )
            self._verify_settings(requested, acknowledged, "acknowledgement")

            readback = await self.async_query_settings()
            self._verify_settings(requested, readback, "readback")
            return readback

    async def _async_prepare_session(self) -> None:
        """Validate or create the App token and obtain fresh MQTT credentials."""
        if self._access_token:
            response = await self._async_api_request(
                "GET", "/pair/list", token=self._access_token, allow_auth_error=True
            )
            if response is None:
                self._access_token = None

        if not self._access_token:
            activated = await self._async_api_request("POST", "/user/activate", data={})
            self._access_token = str(activated["access_token"])
            self._user_id = str(activated.get("user_id") or activated.get("id") or "")
            self._persist_session()

        await self._async_report_homekit_device()
        pair_list = await self._async_api_request(
            "GET", "/pair/list", token=self._access_token
        )
        mqtt = pair_list.get("mqtt")
        if not isinstance(mqtt, dict) or not mqtt.get("brokerUrl"):
            raise HomeAssistantError("Qingping API returned no MQTT configuration")
        self._mqtt = dict(mqtt)
        self._set_error(None)

    async def _async_report_homekit_device(self) -> None:
        metadata = self.homekit_source.accessory_metadata
        if not metadata.get("homekit_unique_id") or not metadata.get("serial_number"):
            raise HomeAssistantError("HomeKit identity is not available")
        payload = {
            "mac": self.mac,
            "model": "Dany",
            "homekit_unique_id": metadata["homekit_unique_id"],
            "sn": metadata["serial_number"],
            "version": metadata.get("firmware", ""),
            "hwVersion": metadata.get("hardware_version", ""),
        }
        await self._async_api_request(
            "POST",
            "/pair/reportHomeKitDevice",
            data=payload,
            token=self._access_token,
        )

    async def _async_api_request(
        self,
        method: str,
        path: str,
        *,
        data: Mapping[str, Any] | None = None,
        token: str | None = None,
        allow_auth_error: bool = False,
    ) -> dict[str, Any] | None:
        timestamp = str(int(time.time()))
        sign_text = "#".join(
            [path, "ios", APP_ID, APP_VERSION, timestamp, self._app_secret]
        )
        headers = {
            "app-timestamp": timestamp,
            "app-platform": "ios",
            "app-version": APP_VERSION,
            "app-build": APP_BUILD,
            "app-flavor": "apple",
            "app-lang": "zh-Hans",
            "app-timezone": self.hass.config.time_zone,
            "app-id": APP_ID,
            "timezone-offset": str(_qingping_timezone_value()),
            "app-temp-unit": "C",
            "app-tvoc-unit": "mg/m3",
            "app-reading-standard": "cn",
            "device-model": "Home Assistant",
            "device-name": "Home Assistant",
            "device-systemManufacturer": "Home Assistant",
            "device-brand": "Home Assistant",
            "device-systemVersion": "2026",
            "device-apiLevel": "2026",
            "auth-type": "leagcy_token" if token else "jwt",
            "jwt-token": "",
            "app-sign": hashlib.md5(sign_text.encode()).hexdigest(),  # noqa: S324
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"

        session = async_get_clientsession(self.hass)
        async with asyncio.timeout(20):
            async with session.request(
                method,
                f"{API_HOST}{path}",
                headers=headers,
                json=dict(data) if data is not None else None,
            ) as response:
                response.raise_for_status()
                body = await response.json(content_type=None)

        if not isinstance(body, dict):
            raise HomeAssistantError("Qingping API returned an invalid response")
        code = int(body.get("code", -1))
        if code == -4769 and allow_auth_error:
            return None
        if code != 0:
            raise HomeAssistantError(
                f"Qingping API {path} failed ({code}): {body.get('desc', '')}"
            )
        result = body.get("data", {})
        if not isinstance(result, dict):
            if result is True:
                return {}
            raise HomeAssistantError(f"Qingping API {path} returned invalid data")
        return result

    async def _async_run_forever(self) -> None:
        retry_delay = 5
        while not self._stop.is_set():
            try:
                if not self._mqtt:
                    await self._async_prepare_session()
                await self._async_mqtt_session()
                retry_delay = 5
            except asyncio.CancelledError:
                raise
            except (ClientError, TimeoutError, HomeAssistantError, ValueError) as err:
                self._set_error(str(err))
                # MQTT URLs and credentials are short-lived. Discard them on
                # every failed session so the next retry obtains a fresh pair
                # list instead of retrying expired credentials indefinitely.
                self._mqtt.clear()
                _LOGGER.warning(
                    "Qingping cloud connection failed for %s: %s", self.mac, err
                )
            finally:
                self._ws = None
                self._set_connected(False)
                self._fail_waiters(
                    HomeAssistantError("Qingping cloud connection was interrupted")
                )

            if self._stop.is_set():
                break
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, 60)

    async def _async_mqtt_session(self) -> None:
        options = self._mqtt.get("options", {})
        required = {
            "url": self._mqtt.get("brokerUrl"),
            "sub_topic": self._mqtt.get("subTopic"),
            "pub_topic": self._mqtt.get("pubTopic"),
            "username": options.get("username"),
            "password": options.get("password"),
            "client_id": options.get("clientId"),
        }
        if not all(required.values()):
            raise HomeAssistantError("Qingping MQTT configuration is incomplete")

        session = async_get_clientsession(self.hass)
        async with session.ws_connect(
            str(required["url"]), protocols=("mqtt",), heartbeat=None
        ) as ws:
            self._ws = ws
            await ws.send_bytes(
                _mqtt_connect_packet(
                    str(required["client_id"]),
                    str(required["username"]),
                    str(required["password"]),
                )
            )
            last_query = 0.0

            while not self._stop.is_set():
                try:
                    message = await asyncio.wait_for(ws.receive(), timeout=30)
                except TimeoutError:
                    await ws.send_bytes(b"\xC0\x00")
                    if (
                        self._connected
                        and not self._response_waiters
                        and time.monotonic() - last_query >= _QUERY_INTERVAL
                    ):
                        await self._async_publish_data({"type": "28"})
                        last_query = time.monotonic()
                    self._publish_state()
                    continue

                if message.type in {WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.ERROR}:
                    raise HomeAssistantError("Qingping MQTT websocket closed")
                if message.type != WSMsgType.BINARY:
                    continue

                packet = bytes(message.data)
                packet_type = packet[0] >> 4
                if packet_type == 2:
                    if len(packet) < 4 or packet[-1] != 0:
                        code = packet[-1] if packet else -1
                        raise HomeAssistantError(
                            f"Qingping MQTT connection rejected ({code})"
                        )
                    await ws.send_bytes(
                        _mqtt_subscribe_packet(str(required["sub_topic"]))
                    )
                elif packet_type == 9:
                    self._set_connected(True)
                    self._set_error(None)
                    await self._async_publish_data({"type": "28"})
                    last_query = time.monotonic()
                elif packet_type == 3:
                    _topic, payload = _decode_publish(packet)
                    await self._async_handle_device_message(payload)

    async def _async_handle_device_message(self, payload: str) -> None:
        outer_mac, response_cid, data = _unwrap_device_message(payload)
        if outer_mac and outer_mac.replace(":", "").upper() != self.mac:
            return

        message_type = str(data.get("type", ""))
        if not message_type:
            return

        self._last_message_monotonic = time.monotonic()
        self._last_message = dt_util.utcnow().isoformat()
        settings = data.get("setting")
        if isinstance(settings, dict):
            self._settings = dict(settings)
            self.runtime[CLOUD_SETTINGS] = dict(settings)
            from .sensor import _update_settings_from_device

            await _update_settings_from_device(
                self.hass,
                self.config_entry,
                settings,
                str(self.config_entry.data.get(CONF_MODEL, "")),
            )

        waiter = self._response_waiters.get(response_cid or "")
        if waiter is not None:
            # An echoed cid is authoritative. Return even a negative or
            # mismatching acknowledgement to the caller so verification can
            # report the actual device response instead of timing out.
            if waiter.response_type == message_type and not waiter.future.done():
                waiter.future.set_result(data)
        else:
            # Some firmware replies omit or replace the outer cid. Fall back
            # only when one request-specific matcher unambiguously accepts the
            # payload; this prevents periodic type 28 queries from stealing a
            # type 17 write acknowledgement.
            matches = [
                pending
                for pending in self._response_waiters.values()
                if not pending.future.done()
                and self._waiter_matches(pending, message_type, data)
            ]
            if len(matches) == 1 and not matches[0].future.done():
                matches[0].future.set_result(data)
        self._publish_state()

    async def _async_send_and_wait(
        self,
        data: Mapping[str, Any],
        response_type: str,
        *,
        matcher: Callable[[Mapping[str, Any]], bool] | None = None,
    ) -> dict[str, Any]:
        if not self._connected or self._ws is None:
            raise HomeAssistantError("Qingping cloud MQTT is not connected")
        cid = secrets.token_hex(8)[:10]
        future = asyncio.get_running_loop().create_future()
        self._response_waiters[cid] = _ResponseWaiter(
            response_type=response_type,
            future=future,
            matcher=matcher,
        )
        try:
            await self._async_publish_data(data, cid=cid)
            async with asyncio.timeout(_DEVICE_RESPONSE_TIMEOUT):
                response = await future
        except TimeoutError as err:
            raise HomeAssistantError(
                f"Qingping device did not answer type {response_type}"
            ) from err
        finally:
            self._response_waiters.pop(cid, None)
        return response

    async def _async_publish_data(
        self, data: Mapping[str, Any], *, cid: str | None = None
    ) -> str:
        if self._ws is None or not self.pub_topic:
            raise HomeAssistantError("Qingping cloud MQTT is not connected")
        request_cid = cid or secrets.token_hex(8)[:10]
        envelope = {
            "cid": request_cid,
            "mac": self.mac,
            "model": "Dany",
            "data": dict(data),
        }
        await self._ws.send_bytes(
            _mqtt_publish_packet(
                self.pub_topic, json.dumps(envelope, separators=(",", ":"))
            )
        )
        return request_cid

    @staticmethod
    def _settings_include(
        data: Mapping[str, Any], expected: Mapping[str, Any]
    ) -> bool:
        """Return whether a response carries all requested setting values."""
        settings = data.get("setting")
        return isinstance(settings, dict) and all(
            settings.get(key) == value for key, value in expected.items()
        )

    @staticmethod
    def _waiter_matches(
        waiter: _ResponseWaiter,
        message_type: str,
        data: Mapping[str, Any],
    ) -> bool:
        """Return whether a payload satisfies a request-specific waiter."""
        if waiter.response_type != message_type:
            return False
        return waiter.matcher is None or waiter.matcher(data)

    @staticmethod
    def _verify_settings(
        expected: Mapping[str, Any], actual: Mapping[str, Any], stage: str
    ) -> None:
        mismatches = {
            key: {"expected": value, "actual": actual.get(key)}
            for key, value in expected.items()
            if actual.get(key) != value
        }
        if mismatches:
            raise HomeAssistantError(
                f"Qingping setting {stage} mismatch: {mismatches}"
            )

    @callback
    def _persist_session(self) -> None:
        data = dict(self.config_entry.data)
        data[CONF_QINGPING_ACCESS_TOKEN] = self._access_token
        data[CONF_QINGPING_USER_ID] = self._user_id
        self.hass.config_entries.async_update_entry(self.config_entry, data=data)

    @callback
    def _set_connected(self, connected: bool) -> None:
        if self._connected == connected:
            return
        self._connected = connected
        self._publish_state()

    @callback
    def _set_error(self, error: str | None) -> None:
        self._last_error = error
        self._publish_state()

    @callback
    def _publish_state(self) -> None:
        self.runtime[CLOUD_AVAILABLE] = self.available
        self.runtime[CLOUD_CONNECTED] = self.connected
        self.runtime[CLOUD_LAST_MESSAGE] = self.last_message
        self.runtime[CLOUD_SETTINGS] = dict(self._settings)
        self.runtime[CLOUD_ERROR] = self.last_error
        self.coordinator.async_set_updated_data(self.runtime)

    @callback
    def _fail_waiters(self, error: Exception) -> None:
        for waiter in self._response_waiters.values():
            if not waiter.future.done():
                waiter.future.set_exception(error)
        self._response_waiters.clear()


def _qingping_timezone_value() -> int:
    """Return the App protocol timezone value (UTC offset hours multiplied by 10)."""
    offset = dt_util.now().utcoffset() or timedelta()
    return int(offset.total_seconds() / 360)


def cloud_source_for_runtime(
    runtime: Mapping[str, Any],
) -> QingpingCloudSource | None:
    source = runtime.get("cloud_source")
    return source if isinstance(source, QingpingCloudSource) else None


async def async_set_cloud_setting(
    hass: HomeAssistant, mac: str, setting_key: str, value: Any
) -> bool:
    """Route a CGDN1 setting to its verified Qingping cloud source."""
    normalized_mac = mac.replace(":", "").upper()
    for runtime in hass.data.get(DOMAIN, {}).values():
        source = cloud_source_for_runtime(runtime)
        if source and source.mac == normalized_mac:
            await source.async_set_settings({setting_key: value})
            return True
    return False
