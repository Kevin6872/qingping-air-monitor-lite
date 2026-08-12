"""Shared entities for Qingping Air Monitor Lite."""
from __future__ import annotations

from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.update_coordinator import (
    CoordinatorEntity,
    DataUpdateCoordinator,
)

from .cloud_source import cloud_source_for_runtime
from .const import DOMAIN
from .homekit_source import device_entry_for_runtime, fallback_device_info


class QingpingLiteEntity(CoordinatorEntity[DataUpdateCoordinator[dict[str, Any]]]):
    """Base entity linked to the existing HomeKit device."""

    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: DataUpdateCoordinator[dict[str, Any]],
        config_entry: ConfigEntry,
    ) -> None:
        super().__init__(coordinator)
        self._config_entry = config_entry
        self._mac = str(config_entry.data["mac"])
        runtime = coordinator.data
        device_entry = device_entry_for_runtime(runtime)
        if device_entry is not None:
            # Helper-style entities can belong to another integration's source
            # device without making that device owned by both config entries.
            self.device_entry = device_entry
        else:
            self._attr_device_info = fallback_device_info(
                self._mac,
                str(config_entry.data.get("name", config_entry.title)),
                str(config_entry.data.get("model", "CGDN1")),
            )

    def _runtime(self) -> dict[str, Any]:
        if self.hass is None:
            return self.coordinator.data
        return self.hass.data[DOMAIN][self._config_entry.entry_id]


class QingpingCloudControlEntity(QingpingLiteEntity):
    """Base class for controls confirmed through Qingping's cloud channel."""

    @property
    def available(self) -> bool:
        source = cloud_source_for_runtime(self._runtime())
        return bool(source and source.available)

    async def async_write_setting(self, key: str, value: Any) -> None:
        source = cloud_source_for_runtime(self._runtime())
        if source is None or not source.available:
            raise HomeAssistantError(
                "Qingping Air Monitor Lite is not responding on the cloud channel"
            )
        await source.async_set_settings({key: value})
