import asyncio
import logging

import av
from av.audio.resampler import AudioResampler
from av.container.input import InputContainer

_LOGGER = logging.getLogger(__name__)


class Stream:
    """
    Audio stream reader (PyAV) that yields 16kHz mono s16le PCM chunks.

    IMPORTANT CHANGE vs upstream:
    - Queue is bounded to prevent unbounded RAM growth (which can trigger HA restarts).
    - When queue is full, we drop the oldest audio chunk(s) to keep "latest audio" behavior.
    """

    # Tune these if needed
    MAX_QUEUE_CHUNKS = 120  # ~a few seconds depending on chunking; prevents OOM
    DROP_CHUNKS_ON_FULL = 20  # drop in batches to recover quickly

    def __init__(self):
        self.closed: bool = False
        self.container: InputContainer | None = None
        self.enabled: bool = False
        self.queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=self.MAX_QUEUE_CHUNKS)

    def open(self, file: str, **kwargs):
        _LOGGER.debug("stream open")

        if "options" not in kwargs:
            kwargs["options"] = {
                "fflags": "nobuffer",
                "flags": "low_delay",
                "timeout": "5000000",
            }

        if file.startswith("rtsp"):
            kwargs["options"]["rtsp_flags"] = "prefer_tcp"

        # audio-only is good (lower CPU/bandwidth), keep it:
        kwargs["options"]["allowed_media_types"] = "audio"

        # av.open also accepts a "timeout" kwarg; keep existing default:
        kwargs.setdefault("timeout", 5)

        # https://pyav.org/docs/9.0.2/api/_globals.html
        self.container = av.open(file, **kwargs)

    def _drop_old_audio(self):
        """Drop some queued chunks to make room (best-effort, non-blocking)."""
        dropped = 0
        while dropped < self.DROP_CHUNKS_ON_FULL:
            try:
                self.queue.get_nowait()
                dropped += 1
            except asyncio.QueueEmpty:
                break

    def run(self, end=True):
        _LOGGER.debug("stream start")

        resampler = AudioResampler(format="s16", layout="mono", rate=16000)

        try:
            # decode audio frames from stream
            for frame in self.container.decode(audio=0):
                if self.closed:
                    return

                # If not actively listening, skip processing to reduce load
                if not self.enabled:
                    continue

                for frame_raw in resampler.resample(frame):
                    chunk = frame_raw.to_ndarray().tobytes()

                    # Prevent unbounded RAM growth:
                    if self.queue.full():
                        self._drop_old_audio()

                    try:
                        self.queue.put_nowait(chunk)
                    except asyncio.QueueFull:
                        # If still full (extreme stall), drop this chunk
                        pass

        except Exception as e:
            _LOGGER.debug(f"stream exception {type(e)}: {e}")

        finally:
            try:
                if self.container:
                    self.container.close()
            except Exception:
                pass

            self.container = None

            # Signal end-of-stream to consumer if we were enabled
            if end and self.enabled:
                # Make sure we can always push the terminator:
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
        # Clear any stale audio
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
