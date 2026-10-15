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
CLOUD_FIRMWARE_INFO = "qingping_cloud_firmware_info"
CLOUD_UPDATE_ERROR = "qingping_cloud_update_error"
CLOUD_UPDATE_IN_PROGRESS = "qingping_cloud_update_in_progress"
CLOUD_UPDATE_PERCENTAGE = "qingping_cloud_update_percentage"

_DEVICE_RESPONSE_TIMEOUT = 15
_DEVICE_AVAILABLE_TIMEOUT = 180
_QUERY_INTERVAL = 90
_MQTT_TICK_INTERVAL = 5
_MQTT_CONNECT_TIMEOUT = 20
_MQTT_PING_INTERVAL = 30
_MQTT_PING_TIMEOUT = 15
_SETTINGS_QUERY_ATTEMPTS = 2
_SETTINGS_QUERY_RETRY_DELAY = 1
_RECONNECT_WAIT_TIMEOUT = 15
_WRITE_ACK_TIMEOUT = 45
_WRITE_READBACK_TIMEOUT = 45
_SETTINGS_READBACK_RETRY_DELAY = 3
_OTA_TIMEOUT = 15 * 60
_VERSION_CONFIRMATION_TIMEOUT = 5 * 60


class _DeviceResponseTimeout(HomeAssistantError):
    """The broker accepted a request but the device did not answer it."""


class _CloudConnectionError(HomeAssistantError):
    """The cloud session must reconnect before another request can run."""


@dataclass
class _MQTTSessionTimers:
    """Deadlines that advance independently of incoming MQTT messages."""

    started_at: float
    next_ping: float
    next_query: float
    ping_deadline: float | None = None


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
    on_message: Callable[[Mapping[str, Any]], bool] | None = None


@dataclass(frozen=True, slots=True)
class FirmwareUpdateInfo:
    """Validated firmware metadata returned by Qingping's update service."""

    version: str
    description: str
    url: str
    file_md5: str
    ota_type: int


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
        self._connected_event = asyncio.Event()
        self._last_message_monotonic: float | None = None
        self._last_message: str | None = None
        self._last_error: str | None = None
        self._settings: dict[str, Any] = {}
        self._firmware_info: FirmwareUpdateInfo | None = None
        self._update_in_progress = False
        self._update_percentage: int | None = None
        self._update_error: str | None = None
        self._response_waiters: dict[str, _ResponseWaiter] = {}
        self._write_lock = asyncio.Lock()
        self._request_lock = asyncio.Lock()

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

    @property
    def firmware_info(self) -> FirmwareUpdateInfo | None:
        """Return the latest validated firmware offer."""
        return self._firmware_info

    @property
    def update_in_progress(self) -> bool:
        """Return whether the device is installing firmware."""
        return self._update_in_progress

    @property
    def update_percentage(self) -> int | None:
        """Return the last OTA progress percentage."""
        return self._update_percentage

    @property
    def update_error(self) -> str | None:
        """Return the last firmware update error."""
        return self._update_error

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
            self._set_error(
                f"Cloud session setup failed: {str(err) or type(err).__name__}"
            )
            _LOGGER.warning("Qingping cloud setup failed for %s: %s", self.mac, err)
        else:
            try:
                await self.async_check_firmware_update()
            except (ClientError, TimeoutError, HomeAssistantError, ValueError) as err:
                self._update_error = str(err)
                self._publish_state()
                _LOGGER.debug(
                    "Qingping firmware check failed for %s: %s", self.mac, err
                )

        self._task = self.config_entry.async_create_background_task(
            self.hass,
            self._async_run_forever(),
            f"{DOMAIN}-{self.mac}-cloud",
        )

    async def async_stop(self) -> None:
        """Stop the cloud connection and fail any waiting service call."""
        self._stop.set()
        self._connected_event.set()
        if self._ws is not None:
            await self._ws.close()
        if self._task is not None:
            self._task.cancel()
            with suppress(asyncio.CancelledError):
                await self._task
            self._task = None
        self._set_connected(False)
        self._fail_waiters(_CloudConnectionError("Qingping cloud connection stopped"))

    async def async_query_settings(self) -> dict[str, Any]:
        """Read fresh settings with bounded retries, never from the cache."""
        for attempt in range(1, _SETTINGS_QUERY_ATTEMPTS + 1):
            try:
                await self._async_wait_connected()
                response = await self._async_send_and_wait(
                    {"type": "28"},
                    "28",
                    matcher=lambda data: isinstance(data.get("setting"), dict),
                )
            except (
                _DeviceResponseTimeout,
                _CloudConnectionError,
                ClientError,
                OSError,
                TimeoutError,
            ) as err:
                if self._stop.is_set():
                    raise _CloudConnectionError("Qingping cloud connection stopped") from err
                _LOGGER.debug(
                    "Qingping settings query %s/%s failed for %s: %s",
                    attempt,
                    _SETTINGS_QUERY_ATTEMPTS,
                    self.mac,
                    str(err) or type(err).__name__,
                )
                if attempt == _SETTINGS_QUERY_ATTEMPTS:
                    reason = (
                        "Qingping settings query failed after "
                        f"{attempt} attempts; refreshing the cloud session"
                    )
                    await self._async_reconnect(reason)
                    raise HomeAssistantError(reason) from err
                await asyncio.sleep(_SETTINGS_QUERY_RETRY_DELAY)
                continue

            settings = response.get("setting")
            if not isinstance(settings, dict):
                raise HomeAssistantError("Qingping device returned no settings")
            return dict(settings)

        raise RuntimeError("Settings query exhausted without a result")

    async def _async_wait_connected(self) -> None:
        """Allow a request to survive a brief automatic reconnect."""
        if self._stop.is_set():
            raise _CloudConnectionError("Qingping cloud connection stopped")
        try:
            async with asyncio.timeout(_RECONNECT_WAIT_TIMEOUT):
                await self._connected_event.wait()
        except TimeoutError as err:
            raise _CloudConnectionError(
                "Timed out waiting for the Qingping cloud connection"
            ) from err
        if self._stop.is_set() or not self._connected:
            raise _CloudConnectionError("Qingping cloud connection was interrupted")

    async def _async_reconnect(self, reason: str) -> None:
        """Invalidate a silent session and let the owner refresh credentials."""
        self._set_error(reason)
        self._set_connected(False)
        self._fail_waiters(_CloudConnectionError(reason))
        if self._ws is not None:
            await self._ws.close()

    async def async_set_settings(self, changes: Mapping[str, Any]) -> dict[str, Any]:
        """Send App-compatible type 17 settings, then query and verify readback."""
        if not changes:
            return dict(self._settings)

        async with self._write_lock:
            requested = dict(changes)
            # Type 17 requires the complete settings object. Always fetch a
            # fresh snapshot immediately before merging so a recent App change
            # is not overwritten by HA's older cache.
            try:
                current = await self.async_query_settings()
            except HomeAssistantError as err:
                raise HomeAssistantError(
                    "Could not read fresh Qingping settings before writing; "
                    f"the requested changes were not sent: {err}"
                ) from err
            merged = {**current, **requested}
            merged.setdefault("is_12_hour_mode", current.get("is_12_hour_mode", 0))
            merged.setdefault("pm25_standard", current.get("pm25_standard", 0))

            try:
                response = await self._async_send_and_wait(
                    {"type": "17", "setting": merged},
                    "28",
                    matcher=lambda data: self._settings_include(data, requested),
                    timeout=_WRITE_ACK_TIMEOUT,
                )
            except (_DeviceResponseTimeout, _CloudConnectionError):
                # The write may already have reached the device. Verify it by
                # reading, rather than blindly sending the full settings twice.
                _LOGGER.debug(
                    "Qingping write acknowledgement missing for %s; checking readback",
                    self.mac,
                )
            else:
                acknowledged = response.get("setting")
                if not isinstance(acknowledged, dict):
                    raise HomeAssistantError(
                        "Qingping device returned no settings acknowledgement"
                    )
                self._verify_settings(requested, acknowledged, "acknowledgement")

            try:
                readback = await self._async_readback_settings(requested)
            except HomeAssistantError as err:
                raise HomeAssistantError(
                    "Qingping settings were sent, but readback failed; "
                    f"the device may already have applied them: {err}"
                ) from err
            self._verify_settings(requested, readback, "readback")
            return readback

    async def _async_readback_settings(
        self, expected: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Allow delayed device reports to settle without replaying the write."""
        last_readback: dict[str, Any] | None = None
        try:
            async with asyncio.timeout(_WRITE_READBACK_TIMEOUT):
                while True:
                    last_readback = await self.async_query_settings()
                    if all(last_readback.get(key) == value for key, value in expected.items()):
                        return last_readback
                    _LOGGER.debug(
                        "Qingping readback still has older settings for %s; waiting for device",
                        self.mac,
                    )
                    await asyncio.sleep(_SETTINGS_READBACK_RETRY_DELAY)
        except TimeoutError as err:
            if last_readback is not None:
                self._verify_settings(expected, last_readback, "readback")
            raise HomeAssistantError("Timed out verifying Qingping settings readback") from err

    async def async_check_firmware_update(self) -> FirmwareUpdateInfo | None:
        """Check Qingping's release channel without starting an OTA."""
        installed_version = self.homekit_source.firmware
        if not installed_version:
            raise HomeAssistantError("HomeKit did not report a firmware version")
        if not self._access_token:
            raise HomeAssistantError("Qingping cloud session is not ready")

        response = await self._async_api_request(
            "GET",
            "/firmware/checkUpdate",
            params={
                "version": installed_version,
                "mac": self.mac,
                "version_type": "release",
                "model": "Dany",
                "homekit": "true",
            },
            token=self._access_token,
        )
        if response is None or int(response.get("upgrade_sign", 0)) != 1:
            self._firmware_info = None
            self._update_error = None
            self._publish_state()
            return None

        info = _firmware_info_from_response(response)
        self._firmware_info = info
        self._update_error = None
        self._publish_state()
        return info

    async def async_install_firmware(self, version: str | None = None) -> None:
        """Install the offered release and verify it through HomeKit."""
        info = self._firmware_info
        if info is None:
            info = await self.async_check_firmware_update()
        if info is None:
            raise HomeAssistantError("No Qingping firmware update is available")
        if version is not None and version != info.version:
            raise HomeAssistantError(
                f"Firmware {version} is not the offered release {info.version}"
            )
        if not self.available or self._ws is None:
            raise HomeAssistantError(
                "Qingping Air Monitor Lite is not responding on the cloud channel"
            )

        async with self._write_lock:
            self._update_in_progress = True
            self._update_percentage = 0
            self._update_error = None
            self._publish_state()
            cid = secrets.token_hex(8)[:10]
            future = asyncio.get_running_loop().create_future()
            self._response_waiters[cid] = _ResponseWaiter(
                response_type="24",
                future=future,
                on_message=self._handle_firmware_progress,
            )
            try:
                await self._async_publish_data(
                    {
                        "type": "23",
                        "url": info.url,
                        "file_md5": info.file_md5,
                        "desc": info.description,
                        "version": info.version,
                        "ota_type": info.ota_type,
                    },
                    cid=cid,
                )
                async with asyncio.timeout(_OTA_TIMEOUT):
                    await future
                await self._async_confirm_firmware_version(info.version)
            except TimeoutError as err:
                error = "Timed out waiting for Qingping firmware installation"
                self._update_error = error
                raise HomeAssistantError(error) from err
            except Exception as err:
                self._update_error = str(err)
                raise
            else:
                self._firmware_info = None
                self._update_error = None
            finally:
                self._response_waiters.pop(cid, None)
                self._update_in_progress = False
                self._update_percentage = None
                self._publish_state()

    def _handle_firmware_progress(self, data: Mapping[str, Any]) -> bool:
        """Record a type-24 OTA report and return whether transfer completed."""
        try:
            error_code = int(data.get("error_code", 0))
        except (TypeError, ValueError) as err:
            raise HomeAssistantError(
                "Qingping returned an invalid OTA error code"
            ) from err
        if error_code != 0:
            raise HomeAssistantError(
                f"Qingping firmware installation failed ({error_code})"
            )

        try:
            percentage = int(data.get("percent", 0))
        except (TypeError, ValueError) as err:
            raise HomeAssistantError("Qingping returned invalid OTA progress") from err
        self._update_percentage = min(100, max(0, percentage))
        self._publish_state()
        return self._update_percentage >= 100

    async def _async_confirm_firmware_version(self, expected: str) -> None:
        """Wait for HomeKit to report the installed firmware version."""
        deadline = time.monotonic() + _VERSION_CONFIRMATION_TIMEOUT
        while time.monotonic() < deadline:
            await self.homekit_source.async_refresh()
            if self.homekit_source.firmware == expected:
                return
            await asyncio.sleep(10)
        raise HomeAssistantError(
            f"Firmware transfer completed but HomeKit did not report {expected}"
        )

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
        params: Mapping[str, Any] | None = None,
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
                params=dict(params) if params is not None else None,
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
            session_started = time.monotonic()
            try:
                if not self._mqtt:
                    await self._async_prepare_session()
                await self._async_mqtt_session()
                retry_delay = 5
            except asyncio.CancelledError:
                raise
            except (ClientError, TimeoutError, HomeAssistantError, ValueError) as err:
                reason = str(err) or type(err).__name__
                self._set_error(reason)
                # MQTT URLs and credentials are short-lived. Discard them on
                # every failed session so the next retry obtains a fresh pair
                # list instead of retrying expired credentials indefinitely.
                self._mqtt.clear()
                _LOGGER.warning(
                    "Qingping cloud connection failed for %s: %s", self.mac, reason
                )
                if (
                    self._last_message_monotonic is not None
                    and self._last_message_monotonic >= session_started
                ):
                    retry_delay = 5
            finally:
                self._ws = None
                self._set_connected(False)
                self._fail_waiters(
                    _CloudConnectionError("Qingping cloud connection was interrupted")
                )

            if self._stop.is_set():
                break
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=retry_delay)
            except TimeoutError:
                pass
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
            self._set_connected(False)
            self._last_message_monotonic = None
            started_at = time.monotonic()
            timers = _MQTTSessionTimers(
                started_at=started_at,
                next_ping=started_at + _MQTT_PING_INTERVAL,
                next_query=started_at,
            )
            await ws.send_bytes(
                _mqtt_connect_packet(
                    str(required["client_id"]),
                    str(required["username"]),
                    str(required["password"]),
                )
            )
            while not self._stop.is_set():
                # Check independent deadlines even if ws.receive() never goes
                # idle. MQTT traffic must not starve PINGREQ or reconciliation.
                await self._async_maintain_mqtt_session(ws, timers)
                try:
                    message = await asyncio.wait_for(
                        ws.receive(), timeout=_MQTT_TICK_INTERVAL
                    )
                except TimeoutError:
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
                    if len(packet) < 5 or packet[-1] == 0x80:
                        raise _CloudConnectionError(
                            "Qingping MQTT subscription rejected"
                        )
                    self._set_connected(True)
                    timers.next_query = time.monotonic()
                elif packet_type == 13:
                    timers.ping_deadline = None
                elif packet_type == 3:
                    _topic, payload = _decode_publish(packet)
                    await self._async_handle_device_message(payload)

    async def _async_maintain_mqtt_session(
        self, ws: ClientWebSocketResponse, timers: _MQTTSessionTimers
    ) -> None:
        """Keep broker and device health independent of incoming traffic."""
        now = time.monotonic()
        if not self._connected and now - timers.started_at >= _MQTT_CONNECT_TIMEOUT:
            raise _CloudConnectionError("Qingping MQTT handshake timed out")
        if timers.ping_deadline is not None and now >= timers.ping_deadline:
            raise _CloudConnectionError("Qingping MQTT heartbeat timed out")

        if now >= timers.next_ping:
            await ws.send_bytes(b"\xC0\x00")
            timers.next_ping = now + _MQTT_PING_INTERVAL
            timers.ping_deadline = now + _MQTT_PING_TIMEOUT
            _LOGGER.debug("Qingping MQTT heartbeat sent for %s", self.mac)

        last_device_message = max(
            timers.started_at, self._last_message_monotonic or timers.started_at
        )
        if (
            self._connected
            and not self._update_in_progress
            and now - last_device_message >= _DEVICE_AVAILABLE_TIMEOUT
        ):
            raise _CloudConnectionError(
                "Qingping device stopped responding; refreshing the cloud session"
            )

        if (
            self._connected
            and now >= timers.next_query
            and not self._write_lock.locked()
            and not self._request_lock.locked()
            and not self._response_waiters
            and not self._update_in_progress
        ):
            await self._async_publish_data({"type": "28"})
            timers.next_query = now + _QUERY_INTERVAL
        self._publish_state()

    async def _async_handle_device_message(self, payload: str) -> None:
        outer_mac, response_cid, data = _unwrap_device_message(payload)
        if outer_mac and outer_mac.replace(":", "").upper() != self.mac:
            return

        message_type = str(data.get("type", ""))
        if not message_type:
            return

        self._last_message_monotonic = time.monotonic()
        self._last_message = dt_util.utcnow().isoformat()
        self._last_error = None
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
            self._deliver_waiter(waiter, message_type, data, exact_cid=True)
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
            if len(matches) == 1:
                self._deliver_waiter(matches[0], message_type, data)
        self._publish_state()

    async def _async_send_and_wait(
        self,
        data: Mapping[str, Any],
        response_type: str,
        *,
        matcher: Callable[[Mapping[str, Any]], bool] | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        async with self._request_lock:
            return await self._async_request(
                data, response_type, matcher=matcher, timeout=timeout
            )

    async def _async_request(
        self,
        data: Mapping[str, Any],
        response_type: str,
        *,
        matcher: Callable[[Mapping[str, Any]], bool] | None = None,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Own one reply waiter, including cleanup after send failures."""
        if not self._connected or self._ws is None:
            raise _CloudConnectionError("Qingping cloud MQTT is not connected")
        cid = secrets.token_hex(8)[:10]
        future = asyncio.get_running_loop().create_future()
        self._response_waiters[cid] = _ResponseWaiter(
            response_type=response_type,
            future=future,
            matcher=matcher,
        )
        try:
            await self._async_publish_data(data, cid=cid)
            async with asyncio.timeout(
                _DEVICE_RESPONSE_TIMEOUT if timeout is None else timeout
            ):
                response = await future
        except TimeoutError as err:
            raise _DeviceResponseTimeout(
                f"Qingping device did not answer type {response_type}"
            ) from err
        finally:
            self._response_waiters.pop(cid, None)
            if not future.done():
                future.cancel()
            elif not future.cancelled():
                # A concurrent disconnect can fail this future while publish
                # itself raises. Retrieve it even when no await was reached.
                future.exception()
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
        _LOGGER.debug(
            "Qingping request type %s sent for %s", data.get("type"), self.mac
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
    def _deliver_waiter(
        waiter: _ResponseWaiter,
        message_type: str,
        data: Mapping[str, Any],
        *,
        exact_cid: bool = False,
    ) -> None:
        """Deliver one matching message, optionally retaining a stream waiter."""
        if (
            waiter.future.done()
            or waiter.response_type != message_type
            or (
                not exact_cid
                and not QingpingCloudSource._waiter_matches(waiter, message_type, data)
            )
        ):
            return
        try:
            completed = waiter.on_message is None or waiter.on_message(data)
        except Exception as err:
            waiter.future.set_exception(err)
            return
        if completed:
            waiter.future.set_result(data)

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
        if connected:
            self._connected_event.set()
        else:
            self._connected_event.clear()
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
        self.runtime[CLOUD_FIRMWARE_INFO] = self._firmware_info
        self.runtime[CLOUD_UPDATE_IN_PROGRESS] = self._update_in_progress
        self.runtime[CLOUD_UPDATE_PERCENTAGE] = self._update_percentage
        self.runtime[CLOUD_UPDATE_ERROR] = self._update_error
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


def _firmware_info_from_response(
    response: Mapping[str, Any],
) -> FirmwareUpdateInfo:
    """Validate firmware metadata before it can reach the physical device."""
    version = str(response.get("version", "")).strip()
    description = str(response.get("desc", "")).strip()
    url = str(response.get("app_url", "")).strip()
    file_md5 = str(response.get("file_md5", "")).strip().lower()
    if not version:
        raise HomeAssistantError("Qingping firmware response has no version")
    if not url.startswith(("https://", "http://")):
        raise HomeAssistantError("Qingping firmware response has an invalid URL")
    if len(file_md5) != 32 or any(
        char not in "0123456789abcdef" for char in file_md5
    ):
        raise HomeAssistantError("Qingping firmware response has an invalid MD5")
    try:
        ota_type = int(response.get("ota_type") or response.get("type") or 0)
    except (TypeError, ValueError) as err:
        raise HomeAssistantError(
            "Qingping firmware response has invalid OTA type"
        ) from err
    return FirmwareUpdateInfo(
        version=version,
        description=description,
        url=url,
        file_md5=file_md5,
        ota_type=ota_type,
    )


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
