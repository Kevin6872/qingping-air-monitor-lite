"""Select controls for Qingping Air Monitor Lite."""
from __future__ import annotations

from typing import Any

from homeassistant.components.select import SelectEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import EntityCategory
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import (
    CONF_IS_12_HOUR_MODE,
    CONF_SCREENSAVER_TYPE,
    CONF_TEMPERATURE_UNIT,
    DOMAIN,
)
from .entity import QingpingCloudControlEntity

SCREENSAVER_OPTIONS = {
    "Rotating readings": 0,
    "Current reading": 1,
    "Time + rotating readings": 2,
    "Time + current reading": 3,
}
TEMPERATURE_UNIT_OPTIONS = {"Celsius (°C)": "C", "Fahrenheit (°F)": "F"}
CLOCK_OPTIONS = {"24-hour": 0, "12-hour": 1}


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    coordinator = hass.data[DOMAIN][config_entry.entry_id]["coordinator"]
    async_add_entities(
        [
            QingpingSettingSelect(coordinator, config_entry, CONF_SCREENSAVER_TYPE, "screensaver_type", SCREENSAVER_OPTIONS, 0),
            QingpingSettingSelect(coordinator, config_entry, CONF_TEMPERATURE_UNIT, "temperature_unit", TEMPERATURE_UNIT_OPTIONS, "C"),
            QingpingSettingSelect(coordinator, config_entry, CONF_IS_12_HOUR_MODE, "clock_format", CLOCK_OPTIONS, 0),
        ]
    )


class QingpingSettingSelect(QingpingCloudControlEntity, SelectEntity):
    """One verified finite type-17 setting."""

    _attr_entity_category = EntityCategory.CONFIG

    def __init__(
        self,
        coordinator,
        config_entry,
        key: str,
        translation_key: str,
        option_values: dict[str, Any],
        default: Any,
    ) -> None:
        super().__init__(coordinator, config_entry)
        self._key = key
        self._option_values = option_values
        self._value_options = {value: option for option, value in option_values.items()}
        self._default = default
        self._attr_unique_id = f"{self._mac}_{key}"
        self._attr_translation_key = translation_key
        self._attr_options = list(option_values)

    @property
    def current_option(self) -> str:
        value = self.coordinator.data.get(self._key, self._default)
        return self._value_options.get(value, self._attr_options[0])

    async def async_select_option(self, option: str) -> None:
        await self.async_write_setting(self._key, self._option_values[option])
