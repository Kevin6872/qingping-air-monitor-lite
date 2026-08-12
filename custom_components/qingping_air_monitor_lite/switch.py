"""Switch controls for Qingping Air Monitor Lite."""
from __future__ import annotations

from homeassistant.components.switch import SwitchEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import EntityCategory
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import CONF_CO2_ASC, DOMAIN
from .entity import QingpingCloudControlEntity


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    coordinator = hass.data[DOMAIN][config_entry.entry_id]["coordinator"]
    async_add_entities([QingpingCo2AscSwitch(coordinator, config_entry)])


class QingpingCo2AscSwitch(QingpingCloudControlEntity, SwitchEntity):
    """CO₂ automatic self-calibration setting."""

    _attr_translation_key = "co2_asc"
    _attr_entity_category = EntityCategory.CONFIG
    _attr_icon = "mdi:molecule-co2"

    def __init__(self, coordinator, config_entry) -> None:
        super().__init__(coordinator, config_entry)
        self._attr_unique_id = f"{self._mac}_{CONF_CO2_ASC}"

    @property
    def is_on(self) -> bool:
        return int(self.coordinator.data.get(CONF_CO2_ASC, 1)) == 1

    async def async_turn_on(self, **kwargs) -> None:
        await self.async_write_setting(CONF_CO2_ASC, 1)

    async def async_turn_off(self, **kwargs) -> None:
        await self.async_write_setting(CONF_CO2_ASC, 0)
