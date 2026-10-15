# Changelog

## 1.2.1

- Keep MQTT heartbeats and periodic settings queries running during continuous incoming traffic.
- Reconnect and refresh credentials after failed broker heartbeats or a silent device.
- Retry read-only settings queries before writes without falling back to stale settings.
- Verify a missing write acknowledgement by readback instead of sending duplicate writes.
- Allow delayed write acknowledgements and temporarily stale readback reports to settle.
- Report whether a failure happened before or after settings were sent.
- Clean up reply futures during send failures and unload, and surface negative correlated acknowledgements.
- Add isolated client regression tests for timers, retries, write confirmation, and cleanup.

## 1.2.0

- Add a Home Assistant firmware update entity linked to the existing HomeKit device.
- Check Qingping's release channel without changing device state.
- Install offered CGDN1 releases through the App-compatible MQTT OTA flow.
- Report OTA progress and confirm the installed version through HomeKit.
- Remove the superseded standalone firmware diagnostic sensor from the entity registry.

## 1.1.0

- Remove the vendor App signing key from distributable source code.
- Add masked setup and reconfigure fields for a user-supplied signing key.
- Preserve existing installations through a one-time local config-entry migration.
- License the repository under GPL-3.0.

## 1.0.2

- Link helper entities to the existing HomeKit device without sharing device ownership.
- Refresh the complete settings snapshot before every write.
- Correlate MQTT replies by request `cid` with a strict compatibility fallback.
- Refresh expired MQTT credentials automatically.
- Validate MAC addresses in the config flow.
- Add the Qingping integration icon.
