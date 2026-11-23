"""Runs the REST and gRPC listeners: as a process, or on a background thread for tests (the object is also the state)."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import threading
from pathlib import Path
from typing import Optional

import grpc
import uvicorn

from .config import MockConfig
from .grpc_server import ZerobusServicer, flight_handler
from .pb import zerobus_service_pb2_grpc as pb_grpc
from .rest import create_rest_app
from .state import MockState

log = logging.getLogger("zerobus_mock_server")


class _Uvicorn(uvicorn.Server):
    """uvicorn without its own signal capture: the mock owns SIGINT and SIGTERM."""

    @contextlib.contextmanager
    def capture_signals(self):  # type: ignore[override]
        yield


class MockZerobus(MockState):
    def __init__(self, cfg: MockConfig) -> None:
        super().__init__(cfg)
        self.rest_app = create_rest_app(self)
        self.servicer = ZerobusServicer(self)
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._ready = threading.Event()
        self._stop_event: Optional[asyncio.Event] = None

    @property
    def rest_base_url(self) -> str:
        return f"https://{self.cfg.host}:{self.cfg.rest_port}"

    @property
    def grpc_endpoint(self) -> str:
        """What a client passes to the SDK; its first host name names the workspace."""
        return f"https://{self.cfg.public_hosts[0]}:{self.cfg.grpc_port}"

    @property
    def unity_catalog_url(self) -> str:
        return f"https://{self.cfg.public_hosts[0]}:{self.cfg.rest_port}"

    @property
    def ca_path(self) -> Path:
        return self.tls.ca_cert

    async def _serve(self, stop_event: asyncio.Event) -> None:
        uv = _Uvicorn(uvicorn.Config(self.rest_app, host=self.cfg.host, port=self.cfg.rest_port,
                                     ssl_certfile=str(self.tls.server_cert), ssl_keyfile=str(self.tls.server_key),
                                     log_level="warning", lifespan="off", access_log=False))
        server = grpc.aio.server(options=[("grpc.max_receive_message_length", 16 << 20),
                                          ("grpc.max_send_message_length", 16 << 20)])
        pb_grpc.add_ZerobusServicer_to_server(self.servicer, server)
        server.add_generic_rpc_handlers((flight_handler(self.servicer),))
        creds = grpc.ssl_server_credentials([(self.tls.server_key.read_bytes(), self.tls.server_cert.read_bytes())])
        server.add_secure_port(f"{self.cfg.host}:{self.cfg.grpc_port}", creds)
        await server.start()
        uv_task = asyncio.ensure_future(uv.serve())
        for _ in range(400):
            if uv.started or uv_task.done():
                break
            await asyncio.sleep(0.025)
        if uv_task.done():
            await server.stop(0)
            uv_task.result()  # re-raises the listener's error
            return
        log.warning("test double: this process mocks the Zerobus Ingest APIs and is not the Zerobus service")
        self._ready.set()
        try:
            await stop_event.wait()
        finally:
            uv.should_exit = True
            with contextlib.suppress(Exception):
                await asyncio.wait_for(uv_task, timeout=5)
            await server.stop(1)

    def start(self, timeout: float = 30.0) -> "MockZerobus":
        """Runs both listeners on a background thread. Returns once they accept connections."""
        self._loop = asyncio.new_event_loop()
        errors: list = []

        def runner() -> None:
            asyncio.set_event_loop(self._loop)
            self._stop_event = asyncio.Event()
            try:
                self._loop.run_until_complete(self._serve(self._stop_event))
            except BaseException as exc:  # noqa: BLE001 - reported to the caller of start()
                errors.append(exc)
                self._ready.set()

        self._thread = threading.Thread(target=runner, name="zerobus-mock", daemon=True)
        self._thread.start()
        if not self._ready.wait(timeout):
            raise TimeoutError("the mock did not start in time")
        if errors:
            raise errors[0]
        return self

    def stop(self, timeout: float = 15.0) -> None:
        if self._loop and self._stop_event is not None and self._thread and self._thread.is_alive():
            self._loop.call_soon_threadsafe(self._stop_event.set)
            self._thread.join(timeout)

    def run_forever(self) -> None:
        """Blocks until SIGINT or SIGTERM."""

        async def main() -> None:
            stop = asyncio.Event()
            for sig in (signal.SIGINT, signal.SIGTERM):
                asyncio.get_running_loop().add_signal_handler(sig, stop.set)
            await self._serve(stop)

        asyncio.run(main())
