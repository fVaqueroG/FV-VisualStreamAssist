import homeassistant.helpers.config_validation as cv
import voluptuous as vol

from homeassistant.components import assist_pipeline
from homeassistant.components.camera import CameraEntityFeature
from homeassistant.components.media_player import MediaPlayerEntityFeature
from homeassistant.config_entries import ConfigFlow, ConfigEntry, OptionsFlow
from homeassistant.core import callback
from homeassistant.helpers import entity_registry

from .core import DOMAIN


def _supports_feature(hass, entity_id: str, feature_flag: int) -> bool:
    state = hass.states.get(entity_id)
    if not state:
        return False
    supported = state.attributes.get("supported_features", 0)
    try:
        return int(supported) & int(feature_flag) != 0
    except Exception:
        return False


def _list_entities_by_domain(reg: entity_registry.EntityRegistry, domain: str) -> list[str]:
    return [entry.entity_id for entry in reg.entities.values() if entry.domain == domain]


class ConfigFlowHandler(ConfigFlow, domain=DOMAIN):
    async def async_step_user(self, user_input=None):
        if user_input:
            title = user_input.pop("name")
            return self.async_create_entry(title=title, data=user_input)

        reg = entity_registry.async_get(self.hass)

        cameras_all = _list_entities_by_domain(reg, "camera")
        cameras = [eid for eid in cameras_all if _supports_feature(self.hass, eid, CameraEntityFeature.STREAM)]

        return self.async_show_form(
            step_id="user",
            data_schema=vol_schema(
                {
                    vol.Required("name"): str,
                    vol.Exclusive("stream_source", "url"): str,
                    vol.Exclusive("camera_entity_id", "url"): vol.In(cameras),
                },
                user_input,
            ),
        )

    @staticmethod
    @callback
    def async_get_options_flow(entry: ConfigEntry):
        # HA 2025.12+: do NOT pass config_entry to the OptionsFlow constructor
        return OptionsFlowHandler()


class OptionsFlowHandler(OptionsFlow):
    async def async_step_init(self, user_input: dict = None):
        if user_input is not None:
            return self.async_create_entry(title="", data=user_input)

        reg = entity_registry.async_get(self.hass)

        cameras_all = _list_entities_by_domain(reg, "camera")
        cameras = [eid for eid in cameras_all if _supports_feature(self.hass, eid, CameraEntityFeature.STREAM)]

        # Kept for possible future use; upstream had multi_select commented out
        players_all = _list_entities_by_domain(reg, "media_player")
        players = [eid for eid in players_all if _supports_feature(self.hass, eid, MediaPlayerEntityFeature.PLAY_MEDIA)]

        pipelines = {p.id: p.name for p in assist_pipeline.async_get_pipelines(self.hass)}
        defaults = dict(self.config_entry.options) if self.config_entry else {}

        return self.async_show_form(
            step_id="init",
            data_schema=vol_schema(
                {
                    vol.Exclusive("stream_source", "url"): str,
                    vol.Exclusive("camera_entity_id", "url"): vol.In(cameras),

                    # upstream uses free-text string (not multi_select)
                    vol.Optional("player_entity_id"): str,

                    vol.Optional("browser_id"): str,
                    vol.Optional("tts_service"): str,
                    vol.Optional("tts_language"): str,
                    vol.Optional("stt_start_media"): str,
                    vol.Optional("speech_gif"): str,
                    vol.Optional("listen_gif"): str,

                    vol.Optional("pipeline_id"): vol.In(pipelines),
                },
                defaults,
            ),
        )


def vol_schema(schema: dict, defaults: dict) -> vol.Schema:
    schema = {k: v for k, v in schema.items() if not empty(v)}

    if defaults:
        for key in schema:
            if key.schema in defaults:
                key.default = vol.default_factory(defaults[key.schema])

    return vol.Schema(schema)


def empty(v) -> bool:
    if isinstance(v, vol.In):
        return len(v.container) == 0

    if isinstance(v, cv.multi_select):
        return len(v.options) == 0

    return False
