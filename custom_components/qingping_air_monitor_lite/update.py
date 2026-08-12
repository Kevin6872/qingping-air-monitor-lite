"""Firmware update support for Qingping Air Monitor Lite."""
from __future__ import annotations

from datetime import timedelta
from typing import Any

from homeassistant.components.update import (
    UpdateDeviceClass,
    UpdateEntity,
    UpdateEntityFeature,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.entity import EntityCategory
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .cloud_source import cloud_source_for_runtime
from .const import DOMAIN
from .entity import QingpingLiteEntity
from .homekit_source import QingpingHomeKitSource

SCAN_INTERVAL = timedelta(hours=6)


async def async_setup_entry(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up the Qingping firmware update entity."""
    coordinator = hass.data[DOMAIN][config_entry.entry_id]["coordinator"]
    async_add_entities([QingpingFirmwareUpdate(coordinator, config_entry)])


class QingpingFirmwareUpdate(QingpingLiteEntity, UpdateEntity):
    """Offer release firmware delivered through Qingping's MQTT channel."""

    _attr_translation_key = "firmware"
    _attr_device_class = UpdateDeviceClass.FIRMWARE
    _attr_entity_category = EntityCategory.CONFIG
    _attr_supported_features = (
        UpdateEntityFeature.INSTALL
        | UpdateEntityFeature.PROGRESS
        | UpdateEntityFeature.RELEASE_NOTES
    )
    _attr_should_poll = True

    def __init__(self, coordinator, config_entry) -> None:
        super().__init__(coordinator, config_entry)
        self._attr_unique_id = f"{self._mac}_firmware_update"

    @property
    def available(self) -> bool:
        runtime = self._runtime()
        homekit_source: QingpingHomeKitSource | None = runtime.get("homekit_source")
        cloud_source = cloud_source_for_runtime(runtime)
        return bool(
            homekit_source
            and homekit_source.available
            and cloud_source
            and cloud_source.available
        )

    @property
    def installed_version(self) -> str | None:
        source: QingpingHomeKitSource | None = self._runtime().get("homekit_source")
        return source.firmware if source else None

    @property
    def latest_version(self) -> str | None:
        source = cloud_source_for_runtime(self._runtime())
        info = source.firmware_info if source else None
        return info.version if info else self.installed_version

    @property
    def release_summary(self) -> str | None:
        source = cloud_source_for_runtime(self._runtime())
        info = source.firmware_info if source else None
        return info.description[:255] if info and info.description else None

    @property
    def in_progress(self) -> bool:
        source = cloud_source_for_runtime(self._runtime())
        return bool(source and source.update_in_progress)

    @property
    def update_percentage(self) -> int | None:
        source = cloud_source_for_runtime(self._runtime())
        return source.update_percentage if source else None

    async def async_update(self) -> None:
        """Refresh release metadata without changing device state."""
        source = cloud_source_for_runtime(self._runtime())
        if source is None:
            raise HomeAssistantError("Qingping cloud source is unavailable")
        await source.async_check_firmware_update()

    async def async_install(
        self,
        version: str | None,
        backup: bool,
        **kwargs: Any,
    ) -> None:
        """Install the latest offered release."""
        if backup:
            raise HomeAssistantError("Qingping firmware backup is not supported")
        source = cloud_source_for_runtime(self._runtime())
        if source is None:
            raise HomeAssistantError("Qingping cloud source is unavailable")
        await source.async_install_firmware(version)

    async def async_release_notes(self) -> str | None:
        """Return the release description supplied by Qingping."""
        source = cloud_source_for_runtime(self._runtime())
        info = source.firmware_info if source else None
        return info.description if info and info.description else None
