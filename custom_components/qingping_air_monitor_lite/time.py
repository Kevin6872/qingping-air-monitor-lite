"""Night-mode time controls for Qingping Air Monitor Lite."""
from __future__ import annotations

from datetime import time

from homeassistant.components.time import TimeEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import EntityCategory
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import (
    CONF_NIGHT_MODE_END_TIME,
    CONF_NIGHT_MODE_START_TIME,
    DOMAIN,
)
from .entity import QingpingCloudControlEntity


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    coordinator = hass.data[DOMAIN][config_entry.entry_id]["coordinator"]
    async_add_entities(
        [
            QingpingNightTime(coordinator, config_entry, CONF_NIGHT_MODE_START_TIME, 0),
            QingpingNightTime(coordinator, config_entry, CONF_NIGHT_MODE_END_TIME, 390),
        ]
    )


class QingpingNightTime(QingpingCloudControlEntity, TimeEntity):
    """One night-mode boundary in minutes since midnight."""

    _attr_entity_category = EntityCategory.CONFIG

    def __init__(self, coordinator, config_entry, key: str, default: int) -> None:
        super().__init__(coordinator, config_entry)
        self._key = key
        self._default = default
        self._attr_unique_id = f"{self._mac}_{key}"
        self._attr_translation_key = key

    @property
    def native_value(self) -> time:
        minutes = int(self.coordinator.data.get(self._key, self._default))
        return time(hour=(minutes // 60) % 24, minute=minutes % 60)

    async def async_set_value(self, value: time) -> None:
        await self.async_write_setting(self._key, value.hour * 60 + value.minute)
