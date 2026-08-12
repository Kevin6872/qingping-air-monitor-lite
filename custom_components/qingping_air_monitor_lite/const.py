"""Constants for the Qingping Air Monitor Lite integration."""

DOMAIN = "qingping_air_monitor_lite"
MODEL = "CGDN1"

CONF_HOMEKIT_PAIRING_ID = "homekit_pairing_id"
CONF_QINGPING_APP_SECRET = "qingping_app_secret"
CONF_QINGPING_ACCESS_TOKEN = "qingping_access_token"
CONF_QINGPING_USER_ID = "qingping_user_id"

SENSOR_BATTERY = "battery"
SENSOR_CO2 = "co2"
SENSOR_HUMIDITY = "humidity"
SENSOR_PM10 = "pm10"
SENSOR_PM25 = "pm25"
SENSOR_TEMPERATURE = "temperature"

CONF_TEMPERATURE_OFFSET = "temperature_offset"
CONF_HUMIDITY_OFFSET = "humidity_offset"
CONF_CO2_OFFSET = "co2_offset"
CONF_PM25_OFFSET = "pm25_offset"
CONF_PM10_OFFSET = "pm10_offset"
CONF_POWER_OFF_TIME = "power_off_time"
CONF_DISPLAY_OFF_TIME = "display_off_time"
CONF_AUTO_SLIDING_TIME = "auto_slideing_time"
CONF_TIMEZONE = "timezone"
CONF_NIGHT_MODE_START_TIME = "night_mode_start_time"
CONF_NIGHT_MODE_END_TIME = "night_mode_end_time"
CONF_SCREENSAVER_TYPE = "screensaver_type"
CONF_TEMPERATURE_UNIT = "temperature_unit"
CONF_IS_12_HOUR_MODE = "is_12_hour_mode"
CONF_CO2_ASC = "co2_asc"

SETTING_KEYS = (
    CONF_TEMPERATURE_OFFSET,
    CONF_HUMIDITY_OFFSET,
    CONF_CO2_OFFSET,
    CONF_PM25_OFFSET,
    CONF_PM10_OFFSET,
    CONF_POWER_OFF_TIME,
    CONF_DISPLAY_OFF_TIME,
    CONF_AUTO_SLIDING_TIME,
    CONF_TIMEZONE,
    CONF_NIGHT_MODE_START_TIME,
    CONF_NIGHT_MODE_END_TIME,
    CONF_SCREENSAVER_TYPE,
    CONF_TEMPERATURE_UNIT,
    CONF_IS_12_HOUR_MODE,
    CONF_CO2_ASC,
)
