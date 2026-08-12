# Changelog

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
