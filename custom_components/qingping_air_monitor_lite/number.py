"""Number controls for Qingping Air Monitor Lite."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from homeassistant.components.number import NumberEntity, NumberMode
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import UnitOfTemperature
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import EntityCategory
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import (
    CONF_AUTO_SLIDING_TIME,
    CONF_CO2_OFFSET,
    CONF_DISPLAY_OFF_TIME,
    CONF_HUMIDITY_OFFSET,
    CONF_PM10_OFFSET,
    CONF_PM25_OFFSET,
    CONF_POWER_OFF_TIME,
    CONF_TEMPERATURE_OFFSET,
    CONF_TIMEZONE,
    DOMAIN,
)
from .entity import QingpingCloudControlEntity


@dataclass(frozen=True, kw_only=True)
class LiteNumberDescription:
    key: str
    translation_key: str
    minimum: float
    maximum: float
    step: float
    default: float
    unit: str | None = None
    to_device: Callable[[float], Any] = float


NUMBER_DESCRIPTIONS = (
    LiteNumberDescription(key=CONF_TEMPERATURE_OFFSET, translation_key="temperature_offset", minimum=-10, maximum=10, step=0.1, default=0, unit=UnitOfTemperature.CELSIUS, to_device=lambda value: int(round(value * 100))),
    LiteNumberDescription(key=CONF_HUMIDITY_OFFSET, translation_key="humidity_offset", minimum=-20, maximum=20, step=0.1, default=0, unit="%", to_device=lambda value: int(round(value * 10))),
    LiteNumberDescription(key=CONF_CO2_OFFSET, translation_key="co2_offset", minimum=-500, maximum=500, step=1, default=0, unit="ppm", to_device=lambda value: int(value)),
    LiteNumberDescription(key=CONF_PM25_OFFSET, translation_key="pm25_offset", minimum=-500, maximum=500, step=1, default=0, unit="µg/m³", to_device=lambda value: int(value)),
    LiteNumberDescription(key=CONF_PM10_OFFSET, translation_key="pm10_offset", minimum=-500, maximum=500, step=1, default=0, unit="µg/m³", to_device=lambda value: int(value)),
    LiteNumberDescription(key=CONF_POWER_OFF_TIME, translation_key="power_off_time", minimum=0, maximum=60, step=5, default=60, unit="min", to_device=lambda value: int(value)),
    LiteNumberDescription(key=CONF_DISPLAY_OFF_TIME, translation_key="display_off_time", minimum=60, maximum=600, step=60, default=600, unit="s", to_device=lambda value: int(value)),
    LiteNumberDescription(key=CONF_AUTO_SLIDING_TIME, translation_key="auto_slideing_time", minimum=60, maximum=600, step=60, default=600, unit="s", to_device=lambda value: int(value)),
    LiteNumberDescription(key=CONF_TIMEZONE, translation_key="timezone", minimum=-12, maximum=14, step=0.5, default=8, unit="h", to_device=lambda value: int(round(value * 10))),
)


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    coordinator = hass.data[DOMAIN][config_entry.entry_id]["coordinator"]
    async_add_entities(
        QingpingSettingNumber(coordinator, config_entry, description)
        for description in NUMBER_DESCRIPTIONS
    )


class QingpingSettingNumber(QingpingCloudControlEntity, NumberEntity):
    """One verified type-17 numeric setting."""

    _attr_entity_category = EntityCategory.CONFIG
    _attr_mode = NumberMode.BOX

    def __init__(self, coordinator, config_entry, description: LiteNumberDescription) -> None:
        super().__init__(coordinator, config_entry)
        self._description = description
        self._attr_unique_id = f"{self._mac}_{description.key}"
        self._attr_translation_key = description.translation_key
        self._attr_native_min_value = description.minimum
        self._attr_native_max_value = description.maximum
        self._attr_native_step = description.step
        self._attr_native_unit_of_measurement = description.unit

    @property
    def native_value(self) -> float:
        return float(self.coordinator.data.get(self._description.key, self._description.default))

    async def async_set_native_value(self, value: float) -> None:
        await self.async_write_setting(
            self._description.key, self._description.to_device(value)
        )
