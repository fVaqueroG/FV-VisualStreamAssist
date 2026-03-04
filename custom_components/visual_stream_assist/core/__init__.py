import asyncio
import logging
from typing import Callable
import time
import re
import random
import io

from mutagen.mp3 import MP3

from homeassistant.components import assist_pipeline
from homeassistant.components import media_player
from homeassistant.components import stt
from homeassistant.components.assist_pipeline import (
    AudioSettings,
    Pipeline,
    PipelineEvent,
    PipelineEventCallback,
    PipelineEventType,
    PipelineInput,
    PipelineStage,
    PipelineRun,
    WakeWordSettings,
)
from homeassistant.components import camera as camera_component
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, Context
from homeassistant.helpers.device_registry import DeviceEntryType
from homeassistant.helpers.entity import Entity, DeviceInfo
from homeassistant.helpers.network import get_url
from homeassistant.helpers.aiohttp_client import async_get_clientsession

from .stream import Stream

_LOGGER = logging.getLogger(__name__)

DOMAIN = "visual_stream_assist"
EVENTS = ["wake", "stt", "intent", "tts"]


def init_entity(entity: Entity, key: str, config_entry: ConfigEntry) -> str:
    unique_id = config_entry.entry_id[:7]
    num = 1 + EVENTS.index(key) if key in EVENTS else 0

    entity._attr_unique_id = f"{unique_id}-{key}"
    entity._attr_name = config_entry.title + " " + key.upper().replace("_", " ")
    entity._attr_icon = f"mdi:numeric-{num}"
    entity._attr_device_info = DeviceInfo(
        name=config_entry.title,
        identifiers={(DOMAIN, unique_id)},
        entry_type=DeviceEntryType.SERVICE,
    )

    return unique_id


async def get_stream_source(hass: HomeAssistant, entity_id: str) -> str | None:
    """Get camera stream source using HA supported API."""
    try:
        return await camera_component.async_get_stream_source(hass, entity_id)
    except Exception as e:
        _LOGGER.error("get_stream_source", exc_info=e)
        return None


async def get_tts_duration(hass: HomeAssistant, tts_url: str) -> float:
    try:
        # Ensure we have the full URL
        if tts_url.startswith("/"):
            base_url = get_url(hass)
            full_url = f"{base_url}{tts_url}"
        else:
            full_url = tts_url

        session = async_get_clientsession(hass)
        async with session.get(full_url) as response:
            if response.status != 200:
                _LOGGER.error(f"Failed to fetch TTS audio: HTTP {response.status}")
                return 0

            content = await response.read()

        audio = MP3(io.BytesIO(content))
        duration = audio.info.length
        _LOGGER.info(f"TTS duration calculated successfully: {duration} seconds")
        return duration

    except Exception as e:
        _LOGGER.error(f"Error getting TTS duration: {e}")
        return 0


async def stream_run(hass: HomeAssistant, data: dict, stt_stream: Stream) -> None:
    stream_kwargs = data.get("stream", {})

    if "file" not in stream_kwargs:
        if url := data.get("stream_source"):
            stream_kwargs["file"] = url
        elif entity := data.get("camera_entity_id"):
            stream_kwargs["file"] = await get_stream_source(hass, entity)
        else:
            return

    stt_stream.open(**stream_kwargs)
    await hass.async_add_executor_job(stt_stream.run)


async def assist_run(
    hass: HomeAssistant,
    data: dict,
    context: Context = None,
    event_callback: PipelineEventCallback = None,
    stt_stream: Stream = None,
) -> dict:
    # 1. Process assist_pipeline settings
    assist = data.get("assist", {})

    if pipeline_id := data.get("pipeline_id"):
        pipeline = assist_pipeline.async_get_pipeline(hass, pipeline_id)
    elif pipeline_json := assist.get("pipeline"):
        pipeline = Pipeline.from_json(pipeline_json)
    else:
        pipeline = assist_pipeline.async_get_pipeline(hass)

    if "start_stage" not in assist:
        if pipeline.wake_word_entity:
            assist["start_stage"] = PipelineStage.WAKE_WORD
        elif pipeline.stt_engine:
            assist["start_stage"] = PipelineStage.STT
        else:
            raise Exception("Unknown start_stage")

    if "end_stage" not in assist:
        if pipeline.tts_engine:
            assist["end_stage"] = PipelineStage.TTS
        else:
            assist["end_stage"] = PipelineStage.INTENT

    player_entity_id = data.get("player_entity_id")
    browser_id = data.get("browser_id")

    # 2. Setup Pipeline Run events
    events = {}

    def internal_event_callback(event: PipelineEvent):
        _LOGGER.debug(f"event: {event}")
        events[event.type] = (
            {"data": event.data, "timestamp": event.timestamp} if event.data else {"timestamp": event.timestamp}
        )

        if event.type == PipelineEventType.WAKE_WORD_END:
            if player_entity_id and (messages_str := data.get("stt_start_media")):
                tts_service = data.get("tts_service")
                tts_language = data.get("tts_language")
                messages = [msg.strip() for msg in messages_str.split(",")]
                random_message = random.choice(messages)
                media_id = f"media-source://tts/{tts_service}?message={random_message}&language={tts_language}"
                play_media(hass, player_entity_id, media_id, "music")

            if player_entity_id and (media_id := data.get("speech_gif")):
                show_popup(hass, player_entity_id, media_id, "picture", browser_id)

            if player_entity_id and (media_id := data.get("listen_gif")):
                asyncio.create_task(async_delay_listening(hass, player_entity_id, media_id, "picture", browser_id))

        elif event.type == PipelineEventType.TTS_END:
            if player_entity_id:
                tts_out = event.data["tts_output"]
                play_media(hass, player_entity_id, tts_out["url"], tts_out["mime_type"])

            if player_entity_id and (media_id := data.get("speech_gif")):
                show_popup(hass, player_entity_id, media_id, "picture", browser_id)

            if player_entity_id:
                tts_out = event.data["tts_output"]
                tts_url = tts_out["url"]
                asyncio.create_task(async_delay_close_popup(hass, player_entity_id, browser_id, tts_url, events))

        if event_callback:
            event_callback(event)

    pipeline_run = PipelineRun(
        hass,
        context=context,
        pipeline=pipeline,
        start_stage=assist["start_stage"],
        end_stage=assist["end_stage"],
        event_callback=internal_event_callback,
        tts_audio_output=assist.get("tts_audio_output"),
        wake_word_settings=new(WakeWordSettings, assist.get("wake_word_settings")),
        audio_settings=new(AudioSettings, assist.get("audio_settings")),
    )

    # 3. Setup Pipeline Input
    pipeline_input = PipelineInput(
        run=pipeline_run,
        stt_metadata=stt.SpeechMetadata(
            language="",
            format=stt.AudioFormats.WAV,
            codec=stt.AudioCodecs.PCM,
            bit_rate=stt.AudioBitRates.BITRATE_16,
            sample_rate=stt.AudioSampleRates.SAMPLERATE_16000,
            channel=stt.AudioChannels.CHANNEL_MONO,
        ),
        stt_stream=stt_stream,
        intent_input=assist.get("intent_input"),
        tts_input=assist.get("tts_input"),
        conversation_id=assist.get("conversation_id"),
        device_id=assist.get("device_id"),
    )

    try:
        await pipeline_input.validate()

        if stt_stream:
            stt_stream.start()

        await pipeline_input.execute()

    except AttributeError:
        pass
    finally:
        if stt_stream:
            stt_stream.stop()

    return events


async def async_delay_listening(hass, player_entity_id, media_id, media_type, browser_id):
    await asyncio.sleep(1.5)
    show_popup(hass, player_entity_id, media_id, media_type, browser_id)


async def async_delay_close_popup(hass, player_entity_id, browser_id, tts_url, events):
    duration = await get_tts_duration(hass, tts_url)
    try:
        events[PipelineEventType.TTS_END]["data"]["tts_duration"] = duration
    except Exception:
        pass
    _LOGGER.debug(f"Stored TTS duration: {duration} seconds")
    await asyncio.sleep(duration)
    close_popup(hass, player_entity_id, browser_id)


def play_media(hass: HomeAssistant, entity_id: str, media_id: str, media_type: str):
    service_data = {
        "entity_id": entity_id,
        "media_content_id": media_player.async_process_play_media_url(hass, media_id),
        "media_content_type": media_type,
    }
    coro = hass.services.async_call("media_player", "play_media", service_data)
    hass.async_create_background_task(coro, "visual_stream_assist_play_media")


def show_popup(hass: HomeAssistant, player_entity_id: str, media_id: str, media_type: str, browser_id: str):
    service_data = {
        "entity_id": player_entity_id,
        "browser_id": browser_id,
        "style": """ --popup-min-width: 800px; --popup-border-radius: 28px; """,
        "content": {"type": media_type, "image": media_id},
    }
    coro = hass.services.async_call("browser_mod", "popup", service_data)
    hass.async_create_background_task(coro, "visual_stream_assist_show_popup")


def close_popup(hass: HomeAssistant, player_entity_id: str, browser_id: str):
    service_data = {"entity_id": player_entity_id, "browser_id": browser_id}
    coro = hass.services.async_call("browser_mod", "close_popup", service_data)
    hass.async_create_background_task(coro, "visual_stream_assist_close_popup")


def run_forever(
    hass: HomeAssistant,
    data: dict,
    context: Context,
    event_callback: PipelineEventCallback,
) -> Callable:
    stt_stream = Stream()

    async def run_stream():
        while not stt_stream.closed:
            try:
                await stream_run(hass, data, stt_stream=stt_stream)
            except Exception as e:
                _LOGGER.debug(f"run_stream error {type(e)}: {e}")
                await asyncio.sleep(30)

    async def run_assist():
        while not stt_stream.closed:
            try:
                await assist_run(
                    hass,
                    data,
                    context=context,
                    event_callback=event_callback,
                    stt_stream=stt_stream,
                )
            except Exception as e:
                _LOGGER.debug(f"run_assist error {type(e)}: {e}")

    hass.async_create_background_task(run_stream(), "visual_stream_assist_run_stream")
    hass.async_create_background_task(run_assist(), "visual_stream_assist_run_assist")

    return stt_stream.close


def new(cls, kwargs: dict):
    if not kwargs:
        return cls()
    kwargs = {k: v for k, v in kwargs.items() if hasattr(cls, k)}
    return cls(**kwargs)
