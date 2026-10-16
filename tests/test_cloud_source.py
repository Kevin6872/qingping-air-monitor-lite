"""Client regressions using HA/transport doubles, without a running HA install."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch


def _load_client():
    """Import the real protocol client while isolating HA-only dependencies."""
    root = Path(__file__).resolve().parents[1] / "custom_components/qingping_air_monitor_lite"
    package_name = "_qingping_cloud_regression"
    package = ModuleType(package_name)
    package.__path__ = [str(root)]

    class HomeAssistantError(Exception):
        pass

    class ClientError(Exception):
        pass

    def module(name, **attrs):
        result = ModuleType(name)
        result.__dict__.update(attrs)
        return result

    async def update_settings(hass, entry, settings, model):
        hass.data["qingping_air_monitor_lite"][entry.entry_id].update(settings)

    stubs = {
        package_name: package,
        f"{package_name}.homekit_source": module("homekit_source", QingpingHomeKitSource=object),
        f"{package_name}.sensor": module("sensor", _update_settings_from_device=update_settings),
        "aiohttp": module("aiohttp", ClientError=ClientError, ClientWebSocketResponse=object,
                          WSMsgType=SimpleNamespace(BINARY=1, CLOSE=2, CLOSED=3, ERROR=4)),
        "homeassistant": module("homeassistant"),
        "homeassistant.config_entries": module("config_entries", ConfigEntry=object),
        "homeassistant.const": module("const", CONF_MAC="mac", CONF_MODEL="model"),
        "homeassistant.core": module("core", HomeAssistant=object, callback=lambda fn: fn),
        "homeassistant.exceptions": module("exceptions", HomeAssistantError=HomeAssistantError),
        "homeassistant.helpers": module("helpers"),
        "homeassistant.helpers.aiohttp_client": module("aiohttp_client", async_get_clientsession=lambda hass: None),
        "homeassistant.helpers.update_coordinator": module("update_coordinator", DataUpdateCoordinator=object),
        "homeassistant.util": module("util", dt=SimpleNamespace(
            utcnow=lambda: datetime.now(timezone.utc),
            now=lambda: datetime.now(timezone.utc),
        )),
    }
    name = f"{package_name}.cloud_source"
    spec = importlib.util.spec_from_file_location(name, root / "cloud_source.py")
    client = importlib.util.module_from_spec(spec)
    # Relative imports during message handling need the two package-local doubles.
    for key in (package_name, f"{package_name}.homekit_source", f"{package_name}.sensor"):
        sys.modules[key] = stubs[key]
    sys.modules[name] = client
    with patch.dict(sys.modules, stubs):
        spec.loader.exec_module(client)
    return client


cloud = _load_client()


class FakeSocket:
    """Advance a fake clock while broker traffic stays continuously busy."""

    def __init__(self, source, clock, *, device_reports=True, ping_replies=True, denied=False):
        self.source = source
        self.clock = clock
        self.device_reports = device_reports
        self.ping_replies = ping_replies
        self.denied = denied
        self.packets = []
        self.index = 0
        self.pending_ping = False
        self.closed = False

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        self.closed = True

    async def send_bytes(self, packet):
        self.packets.append(packet)
        if packet == b"\xc0\x00":
            self.pending_ping = True

    async def receive(self):
        await asyncio.sleep(0)
        self.index += 1
        if self.index == 1:
            packet = b"\x20\x02\x00\x00"
        elif self.index == 2:
            packet = b"\x90\x03\x00\x01" + (b"\x80" if self.denied else b"\x00")
        else:
            self.clock.now += 10
            if self.clock.now >= 240:
                self.source._stop.set()
            if self.pending_ping and self.ping_replies:
                self.pending_ping = False
                packet = b"\xd0\x00"
            elif self.device_reports:
                packet = cloud._mqtt_publish_packet("test", json.dumps({"data": {"type": "10"}}))
            else:
                packet = b"\xd0\x00"
        return SimpleNamespace(type=cloud.WSMsgType.BINARY, data=packet)

    async def close(self):
        self.closed = True

    def queries(self):
        return [json.loads(cloud._decode_publish(packet)[1])["data"]
                for packet in self.packets if packet[0] >> 4 == 3]


class CloudClientTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.entry = SimpleNamespace(entry_id="test", data={
            "mac": "001122334455", "model": "CGDN1", "qingping_app_secret": "test-only",
        })
        self.runtime = {}
        self.hass = SimpleNamespace(data={"qingping_air_monitor_lite": {"test": self.runtime}},
                                    config_entries=SimpleNamespace(async_update_entry=Mock()))
        self.source = cloud.QingpingCloudSource(
            self.hass, self.entry, SimpleNamespace(async_set_updated_data=Mock()),
            self.runtime, SimpleNamespace(firmware="test"),
        )
        self.source._mqtt = {
            "brokerUrl": "wss://example.invalid/mqtt", "subTopic": "test", "pubTopic": "test",
            "options": {"username": "test", "password": "test", "clientId": "test"},
        }

    def connected(self):
        self.source._ws = SimpleNamespace(send_bytes=AsyncMock(), close=AsyncMock())
        self.source._set_connected(True)

    async def run_session(self, **kwargs):
        clock = SimpleNamespace(now=1.0)
        ws = FakeSocket(self.source, clock, **kwargs)
        session = SimpleNamespace(ws_connect=lambda *args, **kw: ws)
        with patch.object(cloud, "time", SimpleNamespace(monotonic=lambda: clock.now)), \
             patch.object(cloud, "async_get_clientsession", return_value=session):
            await self.source._async_mqtt_session()
        return ws

    async def test_busy_receive_stream_does_not_starve_heartbeats_or_queries(self):
        ws = await self.run_session()
        self.assertGreaterEqual(ws.packets.count(b"\xc0\x00"), 6)
        self.assertGreaterEqual(len(ws.queries()), 3)

    async def test_silent_device_forces_reconnect_even_when_broker_answers(self):
        with self.assertRaisesRegex(cloud._CloudConnectionError, "device stopped responding"):
            await self.run_session(device_reports=False)

    async def test_missing_pong_forces_reconnect_despite_device_reports(self):
        with self.assertRaisesRegex(cloud._CloudConnectionError, "heartbeat timed out"):
            await self.run_session(ping_replies=False)

    async def test_failed_subscription_is_not_treated_as_connected(self):
        with self.assertRaisesRegex(cloud._CloudConnectionError, "subscription rejected"):
            await self.run_session(denied=True)
        self.assertFalse(self.source.connected)

    async def test_read_query_retries_and_returns_fresh_values(self):
        self.connected()
        self.source._settings = {"night_mode_end_time": 100}
        self.source._async_send_and_wait = AsyncMock(side_effect=[
            cloud._DeviceResponseTimeout("lost query"),
            {"type": "28", "setting": {"night_mode_end_time": 180}},
        ])
        with patch.object(cloud, "_SETTINGS_QUERY_RETRY_DELAY", 0):
            result = await self.source.async_query_settings()
        self.assertEqual(result["night_mode_end_time"], 180)
        self.assertEqual(self.source._async_send_and_wait.await_count, 2)

    async def test_exhausted_queries_close_the_session_instead_of_using_cache(self):
        self.connected()
        ws = self.source._ws
        self.source._settings = {"night_mode_end_time": 100}
        self.source._async_send_and_wait = AsyncMock(side_effect=cloud._DeviceResponseTimeout("lost query"))
        with patch.object(cloud, "_SETTINGS_QUERY_RETRY_DELAY", 0):
            with self.assertRaisesRegex(cloud.HomeAssistantError, "after 2 attempts"):
                await self.source.async_query_settings()
        ws.close.assert_awaited_once()
        self.assertFalse(self.source.connected)

    async def test_failed_preflight_does_not_send_a_write(self):
        self.source.async_query_settings = AsyncMock(side_effect=cloud.HomeAssistantError("offline"))
        self.source._async_send_and_wait = AsyncMock()
        with self.assertRaisesRegex(cloud.HomeAssistantError, "changes were not sent"):
            await self.source.async_set_settings({"night_mode_end_time": 180})
        self.source._async_send_and_wait.assert_not_called()

    async def test_lost_write_ack_is_checked_without_sending_the_write_twice(self):
        self.source._settings = {"night_mode_end_time": 180}
        self.source.async_query_settings = AsyncMock(side_effect=[
            {"night_mode_end_time": 100, "other_app_setting": 42},
            {"night_mode_end_time": 180, "other_app_setting": 42},
        ])
        self.source._async_send_and_wait = AsyncMock(side_effect=cloud._DeviceResponseTimeout("lost ack"))
        result = await self.source.async_set_settings({"night_mode_end_time": 180})
        self.assertEqual(result["night_mode_end_time"], 180)
        self.source._async_send_and_wait.assert_awaited_once()
        sent = self.source._async_send_and_wait.call_args.args[0]
        self.assertEqual(sent["type"], "17")
        self.assertEqual(sent["setting"]["other_app_setting"], 42)

    async def test_confirmed_unchanged_settings_do_not_send_a_redundant_write(self):
        self.source._settings = {"night_mode_end_time": 100}
        self.source.async_query_settings = AsyncMock(return_value={
            "night_mode_end_time": 180, "other_app_setting": 42,
        })
        self.source._async_send_and_wait = AsyncMock()
        result = await self.source.async_set_settings({"night_mode_end_time": 180})
        self.assertEqual(result["night_mode_end_time"], 180)
        self.source.async_query_settings.assert_awaited_once()
        self.source._async_send_and_wait.assert_not_awaited()

    async def test_a_partly_changed_request_is_not_treated_as_unchanged(self):
        self.source.async_query_settings = AsyncMock(side_effect=[
            {"night_mode_end_time": 180, "night_mode_start_time": 100},
            {"night_mode_end_time": 180, "night_mode_start_time": 300},
        ])
        self.source._async_send_and_wait = AsyncMock(return_value={
            "type": "28", "setting": {"night_mode_end_time": 180, "night_mode_start_time": 300},
        })
        result = await self.source.async_set_settings({
            "night_mode_end_time": 180, "night_mode_start_time": 300,
        })
        self.assertEqual(result["night_mode_start_time"], 300)
        self.source._async_send_and_wait.assert_awaited_once()

    async def test_readback_survives_a_brief_disconnect_without_replaying_the_write(self):
        self.source.async_query_settings = AsyncMock(side_effect=[
            {"night_mode_end_time": 100},
            cloud._CloudConnectionError("reconnecting"),
            {"night_mode_end_time": 180},
        ])
        self.source._async_send_and_wait = AsyncMock(return_value={
            "type": "28", "setting": {"night_mode_end_time": 180},
        })
        with patch.object(cloud, "_SETTINGS_READBACK_RETRY_DELAY", 0):
            result = await self.source.async_set_settings({"night_mode_end_time": 180})
        self.assertEqual(result["night_mode_end_time"], 180)
        self.source._async_send_and_wait.assert_awaited_once()

    async def test_a_permanent_readback_outage_remains_bounded_and_fails(self):
        calls = 0
        async def query():
            nonlocal calls
            calls += 1
            if calls == 1:
                return {"night_mode_end_time": 100}
            raise cloud._CloudConnectionError("still offline")
        self.source.async_query_settings = query
        self.source._async_send_and_wait = AsyncMock(return_value={
            "type": "28", "setting": {"night_mode_end_time": 180},
        })
        with patch.object(cloud, "_WRITE_READBACK_TIMEOUT", 0.01), \
             patch.object(cloud, "_SETTINGS_READBACK_RETRY_DELAY", 0):
            with self.assertRaisesRegex(cloud.HomeAssistantError, "Timed out verifying"):
                await self.source.async_set_settings({"night_mode_end_time": 180})
        self.assertGreater(calls, 2)
        self.source._async_send_and_wait.assert_awaited_once()

    async def test_invalid_readback_is_not_retried_as_a_connection_error(self):
        self.source.async_query_settings = AsyncMock(side_effect=[
            {"night_mode_end_time": 100}, cloud.HomeAssistantError("invalid settings"),
        ])
        self.source._async_send_and_wait = AsyncMock(return_value={
            "type": "28", "setting": {"night_mode_end_time": 180},
        })
        with self.assertRaisesRegex(cloud.HomeAssistantError, "invalid settings"):
            await self.source.async_set_settings({"night_mode_end_time": 180})
        self.assertEqual(self.source.async_query_settings.await_count, 2)

    async def test_unload_does_not_retry_readback_for_the_remaining_window(self):
        calls = 0
        async def query():
            nonlocal calls
            calls += 1
            if calls == 1:
                return {"night_mode_end_time": 100}
            self.source._stop.set()
            raise cloud._CloudConnectionError("stopped")
        self.source.async_query_settings = query
        self.source._async_send_and_wait = AsyncMock(return_value={
            "type": "28", "setting": {"night_mode_end_time": 180},
        })
        with self.assertRaisesRegex(cloud.HomeAssistantError, "stopped"):
            await asyncio.wait_for(self.source.async_set_settings({"night_mode_end_time": 180}), 0.5)
        self.assertEqual(calls, 2)

    async def test_readback_mismatch_is_not_reported_as_success(self):
        self.source.async_query_settings = AsyncMock(return_value={"night_mode_end_time": 100})
        self.source._async_send_and_wait = AsyncMock(side_effect=cloud._DeviceResponseTimeout("lost ack"))
        with patch.object(cloud, "_WRITE_READBACK_TIMEOUT", 0.01), \
             patch.object(cloud, "_SETTINGS_READBACK_RETRY_DELAY", 0):
            with self.assertRaisesRegex(cloud.HomeAssistantError, "readback mismatch"):
                await self.source.async_set_settings({"night_mode_end_time": 180})

    async def test_an_older_report_does_not_fail_a_write_that_settles_later(self):
        self.source.async_query_settings = AsyncMock(side_effect=[
            {"night_mode_end_time": 100},
            {"night_mode_end_time": 100},
            {"night_mode_end_time": 180},
        ])
        self.source._async_send_and_wait = AsyncMock(return_value={
            "type": "28", "setting": {"night_mode_end_time": 180},
        })
        with patch.object(cloud, "_SETTINGS_READBACK_RETRY_DELAY", 0):
            result = await self.source.async_set_settings({"night_mode_end_time": 180})
        self.assertEqual(result["night_mode_end_time"], 180)
        self.source._async_send_and_wait.assert_awaited_once()

    async def test_slow_write_ack_can_arrive_after_the_read_query_timeout(self):
        self.connected()
        self.source.async_query_settings = AsyncMock(side_effect=[
            {"night_mode_end_time": 100}, {"night_mode_end_time": 180},
        ])
        async def publish(data, *, cid=None):
            reply = json.dumps({"cid": cid, "data": {
                "type": "28", "setting": {"night_mode_end_time": 180},
            }})
            asyncio.get_running_loop().call_later(
                0.04, lambda: asyncio.create_task(self.source._async_handle_device_message(reply))
            )
            return cid
        self.source._async_publish_data = publish
        with patch.object(cloud, "_DEVICE_RESPONSE_TIMEOUT", 0.005), \
             patch.object(cloud, "_WRITE_ACK_TIMEOUT", 0.2):
            result = await self.source.async_set_settings({"night_mode_end_time": 180})
        self.assertEqual(result["night_mode_end_time"], 180)

    async def test_exact_cid_returns_a_negative_ack_for_explicit_verification(self):
        self.connected()
        task = asyncio.create_task(self.source._async_send_and_wait(
            {"type": "17"}, "28", matcher=lambda data: data.get("setting", {}).get("value") == 2,
        ))
        await asyncio.sleep(0)
        cid = next(iter(self.source._response_waiters))
        await self.source._async_handle_device_message(json.dumps({
            "cid": cid, "mac": self.source.mac,
            "data": {"type": "28", "setting": {"value": 1}},
        }))
        result = await task
        self.assertEqual(result["setting"]["value"], 1)
        self.assertFalse(self.source._response_waiters)

    async def test_unmatched_reply_cannot_steal_a_write_ack(self):
        self.connected()
        future = asyncio.get_running_loop().create_future()
        self.source._response_waiters["expected"] = cloud._ResponseWaiter(
            "28", future, matcher=lambda data: data.get("setting", {}).get("value") == 2,
        )
        await self.source._async_handle_device_message(json.dumps({
            "cid": "unrelated", "data": {"type": "28", "setting": {"value": 1}},
        }))
        self.assertFalse(future.done())
        future.cancel()

    async def test_send_failure_consumes_a_concurrent_disconnect_exception(self):
        self.connected()
        captured = []
        async def fail_send(*args, **kwargs):
            captured.extend(waiter.future for waiter in self.source._response_waiters.values())
            self.source._fail_waiters(cloud._CloudConnectionError("disconnected"))
            raise OSError("send failed")
        self.source._async_publish_data = fail_send
        with self.assertRaisesRegex(OSError, "send failed"):
            await self.source._async_send_and_wait({"type": "28"}, "28")
        self.assertFalse(self.source._response_waiters)
        self.assertFalse(captured[0]._log_traceback)

    async def test_stop_cancels_background_work_and_releases_waiters(self):
        self.connected()
        ws = self.source._ws
        self.source._task = asyncio.create_task(asyncio.sleep(3600))
        future = asyncio.get_running_loop().create_future()
        self.source._response_waiters["pending"] = cloud._ResponseWaiter("28", future)
        await self.source.async_stop()
        ws.close.assert_awaited_once()
        self.assertIsNone(self.source._task)
        self.assertFalse(self.source.connected)
        self.assertIsInstance(future.exception(), cloud._CloudConnectionError)

    async def test_read_can_wait_for_a_brief_reconnect(self):
        self.source._async_send_and_wait = AsyncMock(return_value={
            "type": "28", "setting": {"night_mode_end_time": 180},
        })
        task = asyncio.create_task(self.source.async_query_settings())
        await asyncio.sleep(0)
        self.assertFalse(task.done())
        self.connected()
        self.assertEqual((await task)["night_mode_end_time"], 180)

    async def test_unload_releases_a_request_waiting_for_connection(self):
        task = asyncio.create_task(self.source.async_query_settings())
        await asyncio.sleep(0)
        await self.source.async_stop()
        with self.assertRaisesRegex(cloud._CloudConnectionError, "stopped"):
            await asyncio.wait_for(task, timeout=0.5)

    async def test_reconnect_refreshes_mqtt_credentials(self):
        self.source._mqtt = {}
        prepared = []
        sessions = []
        async def prepare():
            prepared.append(len(prepared) + 1)
            self.source._mqtt = {"generation": prepared[-1]}
        async def session():
            sessions.append(self.source._mqtt["generation"])
            if len(sessions) == 1:
                raise cloud._CloudConnectionError("heartbeat timed out")
            self.source._stop.set()
        self.source._async_prepare_session = prepare
        self.source._async_mqtt_session = session
        with patch.object(self.source._stop, "wait", AsyncMock(return_value=False)):
            await self.source._async_run_forever()
        self.assertEqual(sessions, [1, 2])


if __name__ == "__main__":
    unittest.main()
