"""Runs the REST and gRPC listeners.

Two ways to run the mock:

* as a process: ``python -m zerobus_mock_server --config config.yaml`` (blocks
  until SIGINT or SIGTERM);
* in-process, for tests: ``MockZerobus(cfg).start()`` runs both listeners on a
  background thread and exposes the stores directly (``.tables``, ``.faults``).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import threading
from pathlib import Path
from typing import Any, Dict, Optional

import grpc
import uvicorn

from . import config as config_mod
from .config import MockConfig
from .faults import Fault
from .grpc_server import ZerobusServicer, flight_handler
from .pb import zerobus_service_pb2_grpc as pb_grpc
from .rest import create_rest_app
from .state import MockState

log = logging.getLogger("zerobus_mock_server")

GRPC_OPTIONS = [
    ("grpc.max_receive_message_length", 16 * 1024 * 1024),
    ("grpc.max_send_message_length", 16 * 1024 * 1024),
]


class _Uvicorn(uvicorn.Server):
    """uvicorn without its own signal capture, so the mock owns SIGINT/SIGTERM."""

    @contextlib.contextmanager
    def capture_signals(self):  # type: ignore[override]
        yield


class MockZerobus:
    def __init__(self, cfg: MockConfig):
        self.cfg = cfg
        self.state = MockState(cfg)
        self.rest_app = create_rest_app(self.state)
        self.servicer = ZerobusServicer(self.state)
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._ready = threading.Event()
        self._error: Optional[BaseException] = None
        self._stop_event: Optional[asyncio.Event] = None
        self._uv: Optional[_Uvicorn] = None

    @classmethod
    def from_config_file(cls, path: Path) -> "MockZerobus":
        return cls(config_mod.load(Path(path)))

    # ------------------------------------------------------------ handles

    @property
    def tables(self):
        return self.state.tables

    @property
    def faults(self):
        return self.state.registry

    @property
    def rest_base_url(self) -> str:
        host = self.cfg.server.host if self.cfg.server.host not in ("0.0.0.0", "") else "127.0.0.1"
        return f"https://{host}:{self.cfg.server.rest_port}"

    @property
    def grpc_endpoint(self) -> str:
        """The endpoint a client passes to the SDK (its first host name names the workspace)."""
        return f"https://{self.cfg.server.public_hosts[0]}:{self.cfg.server.grpc_port}"

    @property
    def unity_catalog_url(self) -> str:
        return f"https://{self.cfg.server.public_hosts[0]}:{self.cfg.server.rest_port}"

    @property
    def ca_path(self) -> Path:
        return self.state.tls.ca_cert

    def add_fault(self, spec: Dict[str, Any]) -> Fault:
        return self.state.registry.add(spec)

    def remove_fault(self, fault_id: str) -> bool:
        return self.state.registry.remove(fault_id)

    def clear_faults(self) -> None:
        self.state.registry.clear()

    def reset(self, tables=None, *, faults: bool = True, schema: bool = False) -> None:
        self.state.reset(tables, faults=faults, schema=schema)

    # ------------------------------------------------------------ serving

    async def _serve(self, stop_event: asyncio.Event) -> None:
        cfg = self.cfg.server
        uv_config = uvicorn.Config(
            self.rest_app,
            host=cfg.host,
            port=cfg.rest_port,
            ssl_certfile=str(self.state.tls.server_cert),
            ssl_keyfile=str(self.state.tls.server_key),
            log_level="warning",
            lifespan="off",
            access_log=False,
        )
        self._uv = _Uvicorn(uv_config)
        server = grpc.aio.server(options=GRPC_OPTIONS)
        pb_grpc.add_ZerobusServicer_to_server(self.servicer, server)
        server.add_generic_rpc_handlers((flight_handler(self.servicer),))
        with open(self.state.tls.server_key, "rb") as kf, open(self.state.tls.server_cert, "rb") as cf:
            creds = grpc.ssl_server_credentials([(kf.read(), cf.read())])
        server.add_secure_port(f"{cfg.host}:{cfg.grpc_port}", creds)
        await server.start()
        uv_task = asyncio.ensure_future(self._uv.serve())
        for _ in range(400):
            if self._uv.started or uv_task.done():
                break
            await asyncio.sleep(0.025)
        if uv_task.done():
            await server.stop(0)
            uv_task.result()  # re-raise the listener error
            return
        log.warning("test double: this process mocks the Zerobus Ingest APIs and is not the Zerobus service")
        log.info("REST (OAuth token, Zerobus insert, /_mock control API) on %s", self.rest_base_url)
        log.info("gRPC (Zerobus EphemeralStream) on %s", self.grpc_endpoint)
        log.info("CA certificate for clients: %s", self.state.tls.ca_cert)
        self._ready.set()
        try:
            await stop_event.wait()
        finally:
            self._uv.should_exit = True
            with contextlib.suppress(Exception):
                await asyncio.wait_for(uv_task, timeout=5)
            await server.stop(1)

    def start(self, timeout: float = 30.0) -> "MockZerobus":
        """Run both listeners on a background thread. Returns self once they accept connections."""
        self._loop = asyncio.new_event_loop()
        ready_err: list = []

        def runner() -> None:
            asyncio.set_event_loop(self._loop)
            self._stop_event = asyncio.Event()
            try:
                self._loop.run_until_complete(self._serve(self._stop_event))
            except BaseException as exc:  # noqa: BLE001 - reported to the caller of start()
                ready_err.append(exc)
                self._ready.set()
            finally:
                self._loop.close()

        self._thread = threading.Thread(target=runner, name="zerobus-mock", daemon=True)
        self._thread.start()
        if not self._ready.wait(timeout):
            raise TimeoutError("the mock did not start in time")
        if ready_err:
            raise ready_err[0]
        return self

    def stop(self, timeout: float = 15.0) -> None:
        if self._loop and self._stop_event is not None and self._thread and self._thread.is_alive():
            self._loop.call_soon_threadsafe(self._stop_event.set)
            self._thread.join(timeout)

    def run_forever(self) -> None:
        """Block until SIGINT or SIGTERM. Used by ``python -m zerobus_mock_server``."""
        import signal

        async def main() -> None:
            stop = asyncio.Event()
            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                with contextlib.suppress(NotImplementedError):
                    loop.add_signal_handler(sig, stop.set)
            await self._serve(stop)

        asyncio.run(main())
