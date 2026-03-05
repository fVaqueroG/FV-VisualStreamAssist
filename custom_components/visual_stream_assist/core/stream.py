import asyncio
import logging
import time

import av
from av.audio.resampler import AudioResampler
from av.container.input import InputContainer

_LOGGER = logging.getLogger(__name__)


class Stream:
    """
    Continuous audio stream reader (PyAV) that yields 16kHz mono s16 PCM chunks.

    Goals:
    - Mic stays ON for wakeword (continuous decode + enqueue)
    - Prevent HA restarts by controlling:
        * memory growth (bounded queue)
        * CPU runaway (backoff + reduced ffmpeg probing + single thread)
        * backlog behavior (drop old audio when consumer lags)
    """

    # --- TUNABLE SAFETY LIMITS ---
    MAX_QUEUE_CHUNKS = 200          # bounded queue (prevents OOM)
    DROP_CHUNKS_ON_FULL = 40        # drop old audio when behind
    OVERLOAD_SLEEP_S = 0.02         # brief backoff when overloaded (CPU saver)
    # -----------------------------

    def __init__(self):
        self.closed: bool = False
        self.container: InputContainer | None = None
        self.enabled: bool = False
        self.queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=self.MAX_QUEUE_CHUNKS)

    def open(self, file: str, **kwargs):
        _LOGGER.debug("stream open")

        # Default options tuned for low-latency + low CPU
        opts = kwargs.get("options") or {}
        opts.setdefault("fflags", "nobuffer")
        opts.setdefault("flags", "low_delay")

        # BIG CPU saver on some streams:
        # reduce probing (less analysis, less cpu) + single thread
        opts.setdefault("analyzeduration", "0")
        opts.setdefault("probesize", "32")
        opts.setdefault("threads", "1")

        # keep audio-only
        opts.setdefault("allowed_media_types", "audio")

        if file.startswith("rtsp"):
            # prefer tcp avoids packet loss churn
            opts.setdefault("rtsp_flags", "prefer_tcp")
            # Some builds also accept:
            opts.setdefault("stimeout", "5000000")  # microseconds

        kwargs["options"] = opts
        kwargs.setdefault("timeout", 5)

        self.container = av.open(file, **kwargs)

    def _drop_old_audio(self):
        dropped = 0
        while dropped < self.DROP_CHUNKS_ON_FULL:
            try:
                self.queue.get_nowait()
                dropped += 1
            except asyncio.QueueEmpty:
                break
        return dropped

    def run(self, end=True):
        """
        Blocking method (runs in executor thread).
        Continuously decodes while enabled=True.
        """
        _LOGGER.debug("stream start")

        resampler = AudioResampler(format="s16", layout="mono", rate=16000)

        try:
            if not self.container:
                return

            for frame in self.container.decode(audio=0):
                if self.closed:
                    return

                # If "mic" disabled, do not enqueue; still decode may be expensive
                # but in your case you want enabled always for wakeword.
                if not self.enabled:
                    time.sleep(0.05)
                    continue

                for frame_raw in resampler.resample(frame):
                    chunk = frame_raw.to_ndarray().tobytes()

                    # If consumer is lagging, drop old audio and briefly back off
                    if self.queue.full():
                        self._drop_old_audio()
                        time.sleep(self.OVERLOAD_SLEEP_S)

                    try:
                        self.queue.put_nowait(chunk)
                    except asyncio.QueueFull:
                        # Extreme stall: drop this chunk and back off
                        time.sleep(self.OVERLOAD_SLEEP_S)

        except Exception as e:
            _LOGGER.debug("stream exception %s: %s", type(e), e)

        finally:
            try:
                if self.container:
                    self.container.close()
            except Exception:
                pass

            self.container = None

            if end and self.enabled:
                # Ensure terminator can be pushed
                if self.queue.full():
                    self._drop_old_audio()
                try:
                    self.queue.put_nowait(b"")
                except asyncio.QueueFull:
                    pass

            _LOGGER.debug("stream end")

    def close(self):
        _LOGGER.debug("stream close")
        self.closed = True

    def start(self):
        # Clear stale audio to start “fresh”
        while True:
            try:
                self.queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        self.enabled = True

    def stop(self):
        self.enabled = False

    def __aiter__(self):
        return self

    async def __anext__(self) -> bytes:
        return await self.queue.get()
