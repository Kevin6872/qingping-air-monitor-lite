"""Diagnostic sensors for Qingping Air Monitor Lite."""
from __future__ import annotations

from typing import Any

from homeassistant.components.sensor import SensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import EntityCategory
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .cloud_source import cloud_source_for_runtime
from .const import (
    CONF_AUTO_SLIDING_TIME,
    CONF_CO2_ASC,
    CONF_CO2_OFFSET,
    CONF_DISPLAY_OFF_TIME,
    CONF_HUMIDITY_OFFSET,
    CONF_IS_12_HOUR_MODE,
    CONF_NIGHT_MODE_END_TIME,
    CONF_NIGHT_MODE_START_TIME,
    CONF_PM10_OFFSET,
    CONF_PM25_OFFSET,
    CONF_POWER_OFF_TIME,
    CONF_SCREENSAVER_TYPE,
    CONF_TEMPERATURE_OFFSET,
    CONF_TEMPERATURE_UNIT,
    CONF_TIMEZONE,
    DOMAIN,
)
from .entity import QingpingLiteEntity
from .homekit_source import QingpingHomeKitSource


async def _update_settings_from_device(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    settings: dict[str, Any],
    _model: str = "CGDN1",
) -> None:
    """Convert a physical type-28 report into canonical HA settings."""
    runtime = hass.data[DOMAIN][config_entry.entry_id]
    conversions = {
        "temperature_offset": (CONF_TEMPERATURE_OFFSET, lambda value: round(float(value) / 100, 2)),
        "humidity_offset": (CONF_HUMIDITY_OFFSET, lambda value: round(float(value) / 10, 1)),
        "co2_offset": (CONF_CO2_OFFSET, int),
        "pm25_offset": (CONF_PM25_OFFSET, int),
        "pm10_offset": (CONF_PM10_OFFSET, int),
        "power_off_time": (CONF_POWER_OFF_TIME, int),
        "display_off_time": (CONF_DISPLAY_OFF_TIME, int),
        "auto_slideing_time": (CONF_AUTO_SLIDING_TIME, int),
        "timezone": (CONF_TIMEZONE, lambda value: round(float(value) / 10, 1)),
        "night_mode_start_time": (CONF_NIGHT_MODE_START_TIME, int),
        "night_mode_end_time": (CONF_NIGHT_MODE_END_TIME, int),
        "screensaver_type": (CONF_SCREENSAVER_TYPE, int),
        "temperature_unit": (CONF_TEMPERATURE_UNIT, str),
        "is_12_hour_mode": (CONF_IS_12_HOUR_MODE, int),
        "co2_asc": (CONF_CO2_ASC, int),
    }
    converted: dict[str, Any] = {}
    for device_key, (setting_key, converter) in conversions.items():
        if device_key not in settings:
            continue
        try:
            converted[setting_key] = converter(settings[device_key])
        except (TypeError, ValueError):
            continue

    if not converted:
        return
    runtime.update(converted)
    new_data = {**config_entry.data, **converted}
    if new_data != config_entry.data:
        hass.config_entries.async_update_entry(config_entry, data=new_data)
    runtime["coordinator"].async_set_updated_data(runtime)


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Lite diagnostics without duplicating HomeKit measurements."""
    coordinator = hass.data[DOMAIN][config_entry.entry_id]["coordinator"]
    async_add_entities(
        [
            QingpingStatusSensor(coordinator, config_entry),
            QingpingMacSensor(coordinator, config_entry),
            QingpingBatteryStateSensor(coordinator, config_entry),
            QingpingCloudStatusSensor(coordinator, config_entry),
        ]
    )


class QingpingStatusSensor(QingpingLiteEntity, SensorEntity):
    """HomeKit transport status."""

    _attr_translation_key = "status"
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_icon = "mdi:home-assistant"

    def __init__(self, coordinator, config_entry) -> None:
        super().__init__(coordinator, config_entry)
        self._attr_unique_id = f"{self._mac}_status"

    @property
    def native_value(self) -> str:
        source: QingpingHomeKitSource | None = self._runtime().get("homekit_source")
        return "online" if source and source.available else "offline"


class QingpingMacSensor(QingpingLiteEntity, SensorEntity):
    """Device MAC used by the Qingping cloud route."""

    _attr_translation_key = "mac_address"
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_icon = "mdi:identifier"

    def __init__(self, coordinator, config_entry) -> None:
        super().__init__(coordinator, config_entry)
        self._attr_unique_id = f"{self._mac}_mac"

    @property
    def native_value(self) -> str:
        return self._mac


class QingpingBatteryStateSensor(QingpingLiteEntity, SensorEntity):
    """Charging state reported by HomeKit."""

    _attr_translation_key = "battery_state"
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_icon = "mdi:battery-charging"

    def __init__(self, coordinator, config_entry) -> None:
        super().__init__(coordinator, config_entry)
        self._attr_unique_id = f"{self._mac}_battery_state"

    @property
    def native_value(self) -> str:
        source: QingpingHomeKitSource | None = self._runtime().get("homekit_source")
        value = source.charging_state if source else None
        return {0: "Discharging", 1: "Charging", 2: "Fully Charged"}.get(
            value, "Unknown"
        )


class QingpingCloudStatusSensor(QingpingLiteEntity, SensorEntity):
    """Qingping App cloud control status."""

    _attr_translation_key = "cloud_status"
    _attr_entity_category = EntityCategory.DIAGNOSTIC

    def __init__(self, coordinator, config_entry) -> None:
        super().__init__(coordinator, config_entry)
        self._attr_unique_id = f"{self._mac}_mqtt_status"

    @property
    def native_value(self) -> str:
        source = cloud_source_for_runtime(self._runtime())
        return "online" if source and source.available else "offline"

    @property
    def icon(self) -> str:
        return "mdi:cloud-check" if self.native_value == "online" else "mdi:cloud-off-outline"

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        source = cloud_source_for_runtime(self._runtime())
        return {
            "channel": "qingping_cloud",
            "broker_connected": bool(source and source.connected),
            "last_device_message": source.last_message if source else None,
            "broker_url": source.broker_url if source else None,
            "last_error": source.last_error if source else None,
        }
