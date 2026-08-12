"""Config flow for Qingping Air Monitor Lite."""
from __future__ import annotations

import re
from typing import Any

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.const import CONF_MAC, CONF_NAME
from homeassistant.data_entry_flow import FlowResult
from homeassistant.helpers.selector import (
    TextSelector,
    TextSelectorConfig,
    TextSelectorType,
)

from .const import (
    CONF_HOMEKIT_PAIRING_ID,
    CONF_QINGPING_APP_SECRET,
    DOMAIN,
    MODEL,
)


def _normalize_mac(value: str) -> str:
    """Normalize and validate a 48-bit MAC address."""
    normalized = re.sub(r"[:-]", "", value.strip()).lower()
    if re.fullmatch(r"[0-9a-f]{12}", normalized) is None:
        raise ValueError("invalid MAC address")
    return normalized


def _normalize_app_secret(value: str) -> str:
    """Validate the user-supplied Qingping App signing key."""
    normalized = value.strip()
    if not normalized:
        raise ValueError("empty Qingping App signing key")
    return normalized


def _secret_selector() -> TextSelector:
    """Return a masked signing-key input without exposing a stored default."""
    return TextSelector(TextSelectorConfig(type=TextSelectorType.PASSWORD))


def _schema(defaults: dict[str, Any] | None = None) -> vol.Schema:
    """Return the user form schema while preserving invalid input."""
    defaults = defaults or {}
    return vol.Schema(
        {
            vol.Required(
                CONF_NAME,
                default=defaults.get(CONF_NAME, "Qingping Air Monitor Lite"),
            ): str,
            vol.Required(CONF_MAC, default=defaults.get(CONF_MAC, vol.UNDEFINED)): str,
            vol.Optional(
                CONF_HOMEKIT_PAIRING_ID,
                default=defaults.get(CONF_HOMEKIT_PAIRING_ID, vol.UNDEFINED),
            ): str,
            vol.Required(CONF_QINGPING_APP_SECRET): _secret_selector(),
        }
    )


class ConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Configure a Qingping Air Monitor Lite."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        errors: dict[str, str] = {}
        if user_input is not None:
            try:
                mac = _normalize_mac(user_input[CONF_MAC])
            except (TypeError, ValueError):
                errors["base"] = "invalid_mac"
            else:
                try:
                    app_secret = _normalize_app_secret(
                        user_input[CONF_QINGPING_APP_SECRET]
                    )
                except (KeyError, TypeError, ValueError):
                    errors["base"] = "invalid_app_secret"
                    app_secret = ""

            if not errors:
                await self.async_set_unique_id(mac)
                self._abort_if_unique_id_configured()
                data = {
                    CONF_NAME: user_input[CONF_NAME],
                    CONF_MAC: mac,
                    CONF_QINGPING_APP_SECRET: app_secret,
                    "model": MODEL,
                }
                pairing_id = user_input.get(CONF_HOMEKIT_PAIRING_ID)
                if pairing_id:
                    data[CONF_HOMEKIT_PAIRING_ID] = pairing_id
                return self.async_create_entry(title=user_input[CONF_NAME], data=data)

        if user_input is None or errors:
            return self.async_show_form(
                step_id="user",
                data_schema=_schema(user_input),
                errors=errors,
            )

        raise RuntimeError("unreachable")

    async def async_step_reconfigure(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Update the signing key without exposing its current value."""
        errors: dict[str, str] = {}
        if user_input is not None:
            try:
                app_secret = _normalize_app_secret(
                    user_input[CONF_QINGPING_APP_SECRET]
                )
            except (KeyError, TypeError, ValueError):
                errors["base"] = "invalid_app_secret"
            else:
                return self.async_update_reload_and_abort(
                    self._get_reconfigure_entry(),
                    data_updates={CONF_QINGPING_APP_SECRET: app_secret},
                )

        return self.async_show_form(
            step_id="reconfigure",
            data_schema=vol.Schema(
                {vol.Required(CONF_QINGPING_APP_SECRET): _secret_selector()}
            ),
            errors=errors,
        )
