"""Reuse Home Assistant's existing HomeKit Device connection."""
from __future__ import annotations

from collections.abc import Callable
from contextlib import suppress
from datetime import timedelta
import logging
from typing import Any

from aiohomekit.model.characteristics import CharacteristicsTypes

from homeassistant.components.homekit_controller.const import (
    IDENTIFIER_ACCESSORY_ID,
    KNOWN_DEVICES,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_MAC, CONF_MODEL, CONF_NAME
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import (
    device_registry as dr,
    entity_registry as er,
    helper_integration,
)
from homeassistant.helpers.event import async_track_time_interval
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator

from .const import (
    CONF_HOMEKIT_PAIRING_ID,
    DOMAIN,
    SENSOR_BATTERY,
    SENSOR_CO2,
    SENSOR_HUMIDITY,
    SENSOR_PM10,
    SENSOR_PM25,
    SENSOR_TEMPERATURE,
)

_LOGGER = logging.getLogger(__name__)

HOMEKIT_VALUES = "homekit_values"
HOMEKIT_AVAILABLE = "homekit_available"
HOMEKIT_FIRMWARE = "homekit_firmware"
HOMEKIT_CHARGING_STATE = "homekit_charging_state"

_CHARACTERISTIC_TO_KEY = {
    str(CharacteristicsTypes.BATTERY_LEVEL).upper(): SENSOR_BATTERY,
    str(CharacteristicsTypes.CARBON_DIOXIDE_LEVEL).upper(): SENSOR_CO2,
    str(CharacteristicsTypes.CHARGING_STATE).upper(): HOMEKIT_CHARGING_STATE,
    str(CharacteristicsTypes.DENSITY_PM10).upper(): SENSOR_PM10,
    str(CharacteristicsTypes.DENSITY_PM25).upper(): SENSOR_PM25,
    str(CharacteristicsTypes.FIRMWARE_REVISION).upper(): HOMEKIT_FIRMWARE,
    str(CharacteristicsTypes.RELATIVE_HUMIDITY_CURRENT).upper(): SENSOR_HUMIDITY,
    str(CharacteristicsTypes.TEMPERATURE_CURRENT).upper(): SENSOR_TEMPERATURE,
}


def _normalize_name(value: str | None) -> str:
    """Normalize a display name for conservative matching."""
    return "".join(char for char in (value or "").casefold() if char.isalnum())


class QingpingHomeKitSource:
    """Read Qingping data through the HomeKit connection owned by HA Core."""

    def __init__(
        self,
        hass: HomeAssistant,
        config_entry: ConfigEntry,
        coordinator: DataUpdateCoordinator,
        runtime: dict[str, Any],
    ) -> None:
        """Initialize the HomeKit data source."""
        self.hass = hass
        self.config_entry = config_entry
        self.coordinator = coordinator
        self.runtime = runtime
        self._connection: Any | None = None
        self._pairing_id: str | None = config_entry.data.get(
            CONF_HOMEKIT_PAIRING_ID
        )
        self._characteristics: dict[tuple[int, int], tuple[str, Any]] = {}
        self._values: dict[str, Any] = {}
        self._available = False
        self._remove_characteristics_listener: Callable[[], None] | None = None
        self._remove_availability_listener: Callable[[], None] | None = None
        self._remove_periodic_refresh: Callable[[], None] | None = None

    @property
    def is_bound(self) -> bool:
        """Return whether an existing HomeKit connection is attached."""
        return self._connection is not None

    @property
    def available(self) -> bool:
        """Return HomeKit connection availability."""
        return self.is_bound and self._available

    @property
    def pairing_id(self) -> str | None:
        """Return the HomeKit accessory pairing identifier."""
        return self._pairing_id

    @property
    def device_entry(self) -> dr.DeviceEntry | None:
        """Return the HA-owned HomeKit device for helper-style entity linking."""
        if not self._pairing_id:
            return None
        registry = dr.async_get(self.hass)
        identifier = (
            IDENTIFIER_ACCESSORY_ID,
            f"{self._pairing_id}:aid:1",
        )
        owner_entry_id = getattr(
            getattr(self._connection, "config_entry", None), "entry_id", None
        )
        scoped_lookup = getattr(registry, "async_get_device_by_identifier", None)
        if scoped_lookup is not None and owner_entry_id:
            return scoped_lookup(identifier, owner_entry_id)
        return registry.async_get_device(identifiers={identifier})

    @property
    def charging_state(self) -> int | None:
        """Return HomeKit Charging State (0, 1, or 2)."""
        value = self._values.get(HOMEKIT_CHARGING_STATE)
        return int(value) if value is not None else None

    @property
    def firmware(self) -> str | None:
        """Return firmware reported by HomeKit."""
        value = self._values.get(HOMEKIT_FIRMWARE)
        return str(value) if value is not None else None

    @property
    def accessory_metadata(self) -> dict[str, str]:
        """Return non-secret identity data from the HA-owned HomeKit pairing."""
        if self._connection is None:
            return {}
        try:
            root = next(
                accessory
                for accessory in self._connection.entity_map.accessories
                if accessory.aid == 1
            )
        except (AttributeError, RuntimeError, StopIteration):
            return {}

        values = {
            "homekit_unique_id": self._pairing_id,
            "model": getattr(root, "model", None),
            "serial_number": getattr(root, "serial_number", None),
            "firmware": self.firmware or getattr(root, "firmware_revision", None),
            "hardware_version": getattr(root, "hardware_revision", None),
        }
        return {key: str(value) for key, value in values.items() if value}

    def supports(self, sensor_type: str) -> bool:
        """Return whether HomeKit exposes this sensor type."""
        return any(key == sensor_type for key, _ in self._characteristics.values())

    def value(self, sensor_type: str) -> Any:
        """Return a cached HomeKit value."""
        return self._values.get(sensor_type)

    async def async_start(self) -> None:
        """Bind and monitor the HomeKit connection without taking ownership."""
        await self._async_ensure_bound()
        self._remove_periodic_refresh = async_track_time_interval(
            self.hass, self._async_periodic_refresh, timedelta(seconds=60)
        )

    async def async_stop(self) -> None:
        """Remove listeners while leaving the HA-owned connection running."""
        if self._remove_periodic_refresh:
            self._remove_periodic_refresh()
            self._remove_periodic_refresh = None
        self._unbind(clear_pairing_id=False)

    async def async_refresh(self) -> None:
        """Read the mapped characteristics through the existing pairing."""
        if not self._connection or not self._characteristics:
            return

        try:
            values = await self._connection.get_characteristics(
                list(self._characteristics)
            )
        except (TimeoutError, OSError) as err:
            _LOGGER.debug("HomeKit refresh failed for %s: %s", self._pairing_id, err)
            self._sync_availability()
            return
        except Exception as err:  # HomeKit transport-specific exceptions
            _LOGGER.debug(
                "Unexpected HomeKit refresh failure for %s: %s",
                self._pairing_id,
                err,
            )
            self._sync_availability()
            return

        for aid_iid, result in values.items():
            if aid_iid not in self._characteristics:
                continue
            if result.get("status", 0) == 0 and "value" in result:
                key, characteristic = self._characteristics[aid_iid]
                self._values[key] = result["value"]
                characteristic.value = result["value"]

        self._sync_availability()
        self._publish_update()

    async def _async_periodic_refresh(self, _now: Any) -> None:
        """Recover from HomeKit entry reloads and reconcile cached state."""
        known_devices = self.hass.data.get(KNOWN_DEVICES, {})
        current = known_devices.get(self._pairing_id) if self._pairing_id else None
        if self._connection is not None and current is not self._connection:
            self._unbind(clear_pairing_id=False)

        if self._connection is None:
            await self._async_ensure_bound()
            return

        await self.async_refresh()

    async def _async_ensure_bound(self) -> bool:
        """Locate the matching HA-owned HomeKit connection and attach listeners."""
        selected = self._find_connection()
        if selected is None:
            self._available = False
            self._publish_update()
            return False

        pairing_id, connection = selected
        if self._connection is connection:
            return True

        self._unbind(clear_pairing_id=False)
        self._connection = connection
        self._pairing_id = pairing_id
        self._map_characteristics()

        characteristic_ids = set(self._characteristics)
        if characteristic_ids:
            self._remove_characteristics_listener = connection.async_subscribe(
                characteristic_ids, self._handle_characteristic_update
            )
        self._remove_availability_listener = connection.async_subscribe_availability(
            self._handle_availability_update
        )

        self._sync_from_cache()
        self._sync_availability()
        self._remember_pairing_id()
        async_migrate_to_homekit_device(
            self.hass,
            self.config_entry,
            self.config_entry.data.get(CONF_MAC, ""),
            self,
        )
        self._publish_update()

        _LOGGER.info(
            "Reusing HomeKit Device connection %s for Qingping %s",
            pairing_id,
            self.config_entry.title,
        )
        await self.async_refresh()
        return True

    def _find_connection(self) -> tuple[str, Any] | None:
        """Find one unambiguous HomeKit connection for this Qingping entry."""
        known_devices: dict[str, Any] = self.hass.data.get(KNOWN_DEVICES, {})
        preferred = self.config_entry.data.get(CONF_HOMEKIT_PAIRING_ID)
        if preferred:
            connection = known_devices.get(preferred)
            if connection is not None:
                return preferred, connection

        expected_model = str(self.config_entry.data.get(CONF_MODEL, "")).casefold()
        expected_name = _normalize_name(self.config_entry.data.get(CONF_NAME))
        candidates: list[tuple[int, str, Any]] = []

        for pairing_id, connection in known_devices.items():
            try:
                accessories = connection.entity_map.accessories
            except (AttributeError, RuntimeError):
                continue

            root = next(
                (accessory for accessory in accessories if accessory.aid == 1), None
            )
            if root is None or str(root.model).casefold() != expected_model:
                continue

            names = {
                _normalize_name(getattr(root, "name", None)),
                _normalize_name(getattr(connection.config_entry, "title", None)),
            }
            score = 1 + (2 if expected_name and expected_name in names else 0)
            candidates.append((score, pairing_id, connection))

        if not candidates:
            return None

        candidates.sort(key=lambda item: item[0], reverse=True)
        if len(candidates) > 1 and candidates[0][0] == candidates[1][0]:
            _LOGGER.warning(
                "Multiple HomeKit devices match Qingping model %s; MQTT remains active",
                expected_model,
            )
            return None

        _, pairing_id, connection = candidates[0]
        return pairing_id, connection

    def _map_characteristics(self) -> None:
        """Map HomeKit aid/iid pairs to Qingping sensor keys."""
        assert self._connection is not None
        self._characteristics.clear()
        for accessory in self._connection.entity_map.accessories:
            for service in accessory.services:
                for characteristic in service.characteristics:
                    key = _CHARACTERISTIC_TO_KEY.get(
                        str(characteristic.type).upper()
                    )
                    if key is not None:
                        self._characteristics[(accessory.aid, characteristic.iid)] = (
                            key,
                            characteristic,
                        )

    @callback
    def _handle_characteristic_update(self) -> None:
        """Read values already applied to aiohomekit's entity map."""
        self._sync_from_cache()
        self._sync_availability()
        self._publish_update()

    @callback
    def _handle_availability_update(self) -> None:
        """Forward HomeKit connection availability."""
        self._sync_availability()
        self._publish_update()

    @callback
    def _sync_from_cache(self) -> None:
        """Copy current characteristic values into canonical runtime state."""
        for key, characteristic in self._characteristics.values():
            if characteristic.value is not None:
                self._values[key] = characteristic.value

    @callback
    def _sync_availability(self) -> None:
        """Read availability from HA Core's connection owner."""
        self._available = bool(
            self._connection is not None and self._connection.available
        )

    @callback
    def _publish_update(self) -> None:
        """Publish canonical HomeKit state through the existing coordinator."""
        self.runtime[HOMEKIT_VALUES] = dict(self._values)
        self.runtime[HOMEKIT_AVAILABLE] = self.available
        self.runtime[CONF_HOMEKIT_PAIRING_ID] = self._pairing_id
        self.coordinator.async_set_updated_data(self.runtime)

    @callback
    def _remember_pairing_id(self) -> None:
        """Persist only the non-secret pairing identifier for future linking."""
        if not self._pairing_id:
            return
        if self.config_entry.data.get(CONF_HOMEKIT_PAIRING_ID) == self._pairing_id:
            return
        self.hass.config_entries.async_update_entry(
            self.config_entry,
            data={**self.config_entry.data, CONF_HOMEKIT_PAIRING_ID: self._pairing_id},
        )

    @callback
    def _unbind(self, *, clear_pairing_id: bool) -> None:
        """Detach our listeners without shutting down the shared pairing."""
        if self._remove_characteristics_listener:
            with suppress(KeyError, ValueError):
                self._remove_characteristics_listener()
            self._remove_characteristics_listener = None
        if self._remove_availability_listener:
            with suppress(KeyError, ValueError):
                self._remove_availability_listener()
            self._remove_availability_listener = None
        self._connection = None
        self._characteristics.clear()
        self._values.clear()
        self._available = False
        if clear_pairing_id:
            self._pairing_id = None


def device_entry_for_runtime(runtime: dict[str, Any]) -> dr.DeviceEntry | None:
    """Return the source HomeKit device for helper-style entity linking."""
    source: QingpingHomeKitSource | None = runtime.get("homekit_source")
    return source.device_entry if source else None


def fallback_device_info(mac: str, name: str, model: str) -> dict[str, Any]:
    """Return an integration-owned device only while HomeKit is unavailable."""
    return {
        "identifiers": {(DOMAIN, mac)},
        "name": name,
        "manufacturer": "Qingping",
        "model": model,
    }


@callback
def async_migrate_to_homekit_device(
    hass: HomeAssistant,
    config_entry: ConfigEntry,
    mac: str,
    source: QingpingHomeKitSource,
) -> None:
    """Migrate legacy shared ownership to helper-style HomeKit device linking."""
    device_registry = dr.async_get(hass)
    entity_registry = er.async_get(hass)
    target = source.device_entry
    if target is None:
        _LOGGER.warning("HomeKit device registry entry was not found for %s", mac)
        return

    # HA <= 2026.7 removes our config entry from the shared device. HA 2026.8+
    # instead removes the helper-owned split/fork and relinks its entities to
    # the concrete HomeKit-owned device. Select the non-deprecated helper when
    # it is available while retaining compatibility with the installed Core.
    remove_helper_devices = getattr(
        helper_integration, "async_remove_helper_devices", None
    )
    if remove_helper_devices is not None:
        remove_helper_devices(
            hass,
            helper_config_entry_id=config_entry.entry_id,
            source_device_id=target.id,
        )
    else:
        helper_integration.async_remove_helper_config_entry_from_source_device(
            hass,
            helper_config_entry_id=config_entry.entry_id,
            source_device_id=target.id,
        )

    scoped_lookup = getattr(device_registry, "async_get_device_by_identifier", None)
    if scoped_lookup is not None:
        old = scoped_lookup((DOMAIN, mac), config_entry.entry_id)
    else:
        old = device_registry.async_get_device(identifiers={(DOMAIN, mac)})

    if old is None or old.id == target.id:
        return

    moved = 0
    for entity in er.async_entries_for_config_entry(
        entity_registry, config_entry.entry_id
    ):
        if entity.device_id == old.id:
            entity_registry.async_update_entity(entity.entity_id, device_id=target.id)
            moved += 1

    remaining = er.async_entries_for_device(
        entity_registry, old.id, include_disabled_entities=True
    )
    old_config_entry_id = getattr(old, "config_entry_id", None)
    owned_only_by_us = (
        old_config_entry_id == config_entry.entry_id
        if old_config_entry_id is not None
        else set(getattr(old, "config_entries", ())) <= {config_entry.entry_id}
    )
    if not remaining and owned_only_by_us:
        device_registry.async_remove_device(old.id)
        _LOGGER.info(
            "Moved %s Qingping entities to HomeKit device %s and removed duplicate %s",
            moved,
            target.id,
            old.id,
        )
    else:
        _LOGGER.warning(
            "Moved %s Qingping entities but kept duplicate device %s because "
            "it still has references",
            moved,
            old.id,
        )
