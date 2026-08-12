"""Qingping Air Monitor Lite integration."""
from __future__ import annotations

import logging
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator

from .cloud_source import QingpingCloudSource
from .const import DOMAIN, SETTING_KEYS
from .homekit_source import QingpingHomeKitSource, async_migrate_to_homekit_device

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[Platform] = [
    Platform.SENSOR,
    Platform.NUMBER,
    Platform.SELECT,
    Platform.SWITCH,
    Platform.TIME,
    Platform.UPDATE,
]


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up one Qingping Air Monitor Lite."""
    entity_registry = er.async_get(hass)
    legacy_unique_id = f"{entry.data.get('mac', '')}_firmware"
    for entity in er.async_entries_for_config_entry(entity_registry, entry.entry_id):
        if entity.platform == DOMAIN and entity.unique_id == legacy_unique_id:
            entity_registry.async_remove(entity.entity_id)

    hass.data.setdefault(DOMAIN, {})
    coordinator: DataUpdateCoordinator[dict[str, Any]] = DataUpdateCoordinator(
        hass,
        _LOGGER,
        name=f"{DOMAIN}-{entry.entry_id}",
    )
    runtime: dict[str, Any] = {
        "coordinator": coordinator,
        **{key: entry.data[key] for key in SETTING_KEYS if key in entry.data},
    }
    hass.data[DOMAIN][entry.entry_id] = runtime
    coordinator.async_set_updated_data(runtime)

    homekit_source = QingpingHomeKitSource(hass, entry, coordinator, runtime)
    runtime["homekit_source"] = homekit_source
    await homekit_source.async_start()

    cloud_source = QingpingCloudSource(
        hass, entry, coordinator, runtime, homekit_source
    )
    runtime["cloud_source"] = cloud_source
    await cloud_source.async_start()

    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    async_migrate_to_homekit_device(
        hass, entry, entry.data.get("mac", ""), homekit_source
    )
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload one Qingping Air Monitor Lite."""
    runtime = hass.data.get(DOMAIN, {}).get(entry.entry_id, {})
    if not await hass.config_entries.async_unload_platforms(entry, PLATFORMS):
        return False

    cloud_source = runtime.get("cloud_source")
    if cloud_source:
        await cloud_source.async_stop()
    homekit_source = runtime.get("homekit_source")
    if homekit_source:
        await homekit_source.async_stop()
    hass.data[DOMAIN].pop(entry.entry_id, None)
    return True
