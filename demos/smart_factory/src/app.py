"""
SmartFactory FastAPI Application.

Serves the React frontend, runs the IoT sensor simulator,
pushes data via ZeroBus, and streams updates to the UI via WebSocket.
"""

import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from databricks.sdk import WorkspaceClient

from src.simulator import SensorSimulator, get_machine_configs
from src.zerobus_client import ZeroBusClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# --- WebSocket Connection Manager ---

class ConnectionManager:
    """Manages WebSocket connections for real-time UI updates."""

    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket connected. Total: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        logger.info(f"WebSocket disconnected. Total: {len(self.active_connections)}")

    async def broadcast(self, message: dict):
        dead = []
        for conn in self.active_connections:
            try:
                await conn.send_json(message)
            except Exception:
                dead.append(conn)
        for conn in dead:
            self.active_connections.remove(conn)


manager = ConnectionManager()
simulator = SensorSimulator()
zerobus: ZeroBusClient | None = None
simulator_task: asyncio.Task | None = None
workspace_client: WorkspaceClient | None = None
pipeline_id: str | None = None


# --- Simulator Loop ---

async def run_simulator():
    """Background loop: generate sensor data, push to ZeroBus + WebSocket."""
    interval_ms = int(os.getenv("SIMULATOR_INTERVAL_MS", "2000"))
    interval_s = interval_ms / 1000

    while True:
        try:
            events = simulator.generate_tick()

            # Push to ZeroBus (async, for pipeline ingestion)
            if zerobus:
                await zerobus.push_events(events)

            # Push to WebSocket (instant UI update)
            await manager.broadcast({
                "type": "sensor_data",
                "events": events,
                "fault_states": simulator.get_fault_states(),
            })

        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Simulator tick error: {e}")

        await asyncio.sleep(interval_s)


# --- Pipeline Management ---

def _find_pipeline_id() -> str | None:
    """Find the SmartFactory SDP pipeline ID."""
    global pipeline_id
    if pipeline_id:
        return pipeline_id
    # Check env var first
    env_id = os.getenv("PIPELINE_ID")
    if env_id:
        pipeline_id = env_id
        return pipeline_id
    if not workspace_client:
        return None
    try:
        pipelines = workspace_client.pipelines.list_pipelines(
            filter="name LIKE 'smartfactory-sdp'"
        )
        for p in pipelines:
            pipeline_id = p.pipeline_id
            logger.info(f"Found pipeline: {p.name} ({pipeline_id})")
            return pipeline_id
    except Exception as e:
        logger.error(f"Failed to find pipeline: {e}")
    return None


def _get_pipeline_status() -> dict:
    """Get current pipeline status."""
    pid = _find_pipeline_id()
    if not pid or not workspace_client:
        return {"state": "NOT_FOUND", "pipeline_id": None}
    try:
        p = workspace_client.pipelines.get(pipeline_id=pid)
        return {
            "state": p.state.value if p.state else "UNKNOWN",
            "pipeline_id": pid,
            "name": p.name,
            "last_update": p.latest_updates[0].update_id if p.latest_updates else None,
        }
    except Exception as e:
        logger.error(f"Failed to get pipeline status: {e}")
        return {"state": "ERROR", "pipeline_id": pid, "error": str(e)}


# --- App Lifespan ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    global zerobus, simulator_task, workspace_client

    # Startup
    enable_sim = os.getenv("ENABLE_SIMULATOR", "true").lower() == "true"

    try:
        workspace_client = WorkspaceClient()
        logger.info("Workspace client initialized")
        # Self-heal: ensure SP has warehouse access (gets dropped by bundle deploy)
        if WAREHOUSE_ID:
            try:
                sp_id = os.getenv("DATABRICKS_CLIENT_ID")
                if sp_id:
                    workspace_client.warehouses.set_permissions(
                        warehouse_id=WAREHOUSE_ID,
                        access_control_list=[{
                            "service_principal_name": sp_id,
                            "permission_level": "CAN_USE",
                        }],
                    )
                    logger.info(f"Warehouse permission confirmed for SP {sp_id}")
            except Exception as e:
                logger.warning(f"Could not set warehouse permission: {e}")
    except Exception as e:
        logger.warning(f"Workspace client init failed: {e}")

    try:
        zerobus = ZeroBusClient()
        logger.info("ZeroBus client initialized")
    except Exception as e:
        logger.warning(f"ZeroBus client init failed: {e}. Running without ingestion.")
        zerobus = None

    if enable_sim:
        simulator_task = asyncio.create_task(run_simulator())
        logger.info("Sensor simulator started")

    # Start dashboard cache refresh
    global cache_task
    cache_task = asyncio.create_task(_refresh_dashboard_cache())
    logger.info("Dashboard cache started (5s refresh)")

    yield

    # Shutdown
    if simulator_task:
        simulator_task.cancel()
        try:
            await simulator_task
        except asyncio.CancelledError:
            pass
    if cache_task:
        cache_task.cancel()
        try:
            await cache_task
        except asyncio.CancelledError:
            pass
    logger.info("SmartFactory app shutdown complete")


# --- FastAPI App ---

app = FastAPI(title="SmartFactory", lifespan=lifespan)


# --- API Models ---

class FaultRequest(BaseModel):
    machine_id: str


# --- API Endpoints ---

@app.get("/api/machines")
async def get_machines():
    """Return machine configurations and current fault states."""
    return {
        "machines": get_machine_configs(),
        "fault_states": simulator.get_fault_states(),
    }


@app.get("/api/status")
async def get_status():
    """Return current simulator status."""
    return {
        "simulator_running": simulator_task is not None and not simulator_task.done(),
        "zerobus_connected": zerobus is not None,
        "websocket_connections": len(manager.active_connections),
        "fault_states": simulator.get_fault_states(),
    }


@app.post("/api/fault/inject")
async def inject_fault(req: FaultRequest):
    """Inject a fault into a specific machine."""
    success = simulator.inject_fault(req.machine_id)
    if not success:
        return {"error": f"Unknown machine: {req.machine_id}"}, 404
    logger.info(f"Fault injected: {req.machine_id}")
    return {"status": "fault_injected", "machine_id": req.machine_id}


@app.post("/api/fault/clear")
async def clear_fault(req: FaultRequest):
    """Clear a fault from a specific machine."""
    success = simulator.clear_fault(req.machine_id)
    if not success:
        return {"error": f"Unknown machine: {req.machine_id}"}, 404
    logger.info(f"Fault cleared: {req.machine_id}")
    return {"status": "fault_cleared", "machine_id": req.machine_id}


@app.post("/api/fault/clear-all")
async def clear_all_faults():
    """Clear all faults."""
    simulator.clear_all_faults()
    logger.info("All faults cleared")
    return {"status": "all_faults_cleared"}


# --- Simulator Control Endpoints ---

@app.get("/api/simulator/status")
async def get_simulator_status():
    """Get simulator (data streaming) status."""
    running = simulator_task is not None and not simulator_task.done()
    return {"running": running}


@app.post("/api/simulator/start")
async def start_simulator():
    """Start the sensor data simulator."""
    global simulator_task
    if simulator_task and not simulator_task.done():
        return {"status": "already_running"}
    simulator_task = asyncio.create_task(run_simulator())
    logger.info("Simulator started via API")
    return {"status": "started"}


@app.post("/api/simulator/stop")
async def stop_simulator():
    """Stop the sensor data simulator."""
    global simulator_task
    if not simulator_task or simulator_task.done():
        return {"status": "already_stopped"}
    simulator_task.cancel()
    try:
        await simulator_task
    except asyncio.CancelledError:
        pass
    simulator_task = None
    logger.info("Simulator stopped via API")
    return {"status": "stopped"}


@app.post("/api/reset-data")
async def reset_data():
    """Reset demo by stopping pipeline, clearing data, and doing a full refresh."""
    if not workspace_client or not WAREHOUSE_ID:
        return {"error": "No workspace client or warehouse"}
    try:
        # Stop pipeline first so it releases the streaming source
        pid = _find_pipeline_id()
        if pid:
            try:
                workspace_client.pipelines.stop(pipeline_id=pid)
                logger.info("Pipeline stopped for reset")
                await asyncio.sleep(10)  # Wait for pipeline to fully stop
            except Exception:
                pass

        # Now safe to truncate the landing table
        workspace_client.statement_execution.execute_statement(
            statement=f"TRUNCATE TABLE {CATALOG}.smartfactory.raw_sensor_events",
            warehouse_id=WAREHOUSE_ID,
        )
        logger.info("Landing table truncated")

        # Trigger a full refresh of the pipeline (resets streaming checkpoints)
        pid = _find_pipeline_id()
        if pid:
            workspace_client.pipelines.start_update(
                pipeline_id=pid,
                full_refresh=True,
            )
            logger.info(f"Pipeline full refresh triggered: {pid}")

        # Clear simulator fault state
        simulator.clear_all_faults()
        logger.info("Demo data reset complete")
        return {"status": "reset_complete"}
    except Exception as e:
        logger.error(f"Reset failed: {e}")
        return {"error": str(e)}


# --- Pipeline Endpoints ---

@app.get("/api/pipeline/status")
async def get_pipeline_status():
    """Get SDP pipeline status."""
    return _get_pipeline_status()


@app.post("/api/pipeline/start")
async def start_pipeline():
    """Start/trigger the SDP pipeline."""
    pid = _find_pipeline_id()
    if not pid or not workspace_client:
        return {"error": "Pipeline not found"}, 404
    try:
        update = workspace_client.pipelines.start_update(pipeline_id=pid)
        logger.info(f"Pipeline started: {pid}")
        return {"status": "started", "pipeline_id": pid, "update_id": update.update_id}
    except Exception as e:
        logger.error(f"Failed to start pipeline: {e}")
        return {"error": str(e)}


@app.post("/api/pipeline/stop")
async def stop_pipeline():
    """Stop the SDP pipeline."""
    pid = _find_pipeline_id()
    if not pid or not workspace_client:
        return {"error": "Pipeline not found"}, 404
    try:
        workspace_client.pipelines.stop(pipeline_id=pid)
        logger.info(f"Pipeline stopped: {pid}")
        return {"status": "stopped", "pipeline_id": pid}
    except Exception as e:
        logger.error(f"Failed to stop pipeline: {e}")
        return {"error": str(e)}


# --- Dashboard Data Cache ---

WAREHOUSE_ID = os.getenv("WAREHOUSE_ID")
CATALOG = os.getenv("CATALOG_NAME", "dilan_catalog")
PIPELINE_SCHEMA = os.getenv("PIPELINE_SCHEMA", "dev_dilan_patel_smartfactory")

# Background cache — warehouse queries run here, frontend reads from cache
dashboard_cache: dict = {
    "health": [],
    "kpis": [],
    "anomalies": [],
    "trends": [],
}
cache_task: asyncio.Task | None = None


def _run_sql(query: str) -> list[dict]:
    """Execute SQL and return results as list of dicts."""
    if not workspace_client or not WAREHOUSE_ID:
        return []
    try:
        result = workspace_client.statement_execution.execute_statement(
            statement=query,
            warehouse_id=WAREHOUSE_ID,
        )
        if not result.result or not result.result.data_array:
            return []
        columns = [c.name for c in result.manifest.schema.columns]
        return [dict(zip(columns, row)) for row in result.result.data_array]
    except Exception as e:
        logger.error(f"SQL query failed: {e}")
        return []


async def _refresh_dashboard_cache():
    """Background loop: refresh dashboard data from warehouse every 5 seconds."""
    while True:
        try:
            # Run all queries sequentially (one at a time, not competing)
            health = _run_sql(f"""
                WITH kpis AS (
                    SELECT machine_id, machine_type, sensor_name,
                        COUNT(CASE WHEN anomaly_status = 'CRITICAL' THEN 1 END) AS critical_count,
                        COUNT(CASE WHEN anomaly_status = 'WARNING' THEN 1 END) AS warning_count,
                        GREATEST(0, 100
                            - COUNT(CASE WHEN anomaly_status = 'CRITICAL' THEN 1 END) * 10
                            - COUNT(CASE WHEN anomaly_status = 'WARNING' THEN 1 END) * 3
                        ) AS health_score
                    FROM {CATALOG}.{PIPELINE_SCHEMA}.enriched_events
                    WHERE TRUE
                    GROUP BY machine_id, machine_type, sensor_name
                )
                SELECT machine_id, machine_type,
                    MIN(health_score) AS worst_sensor_health,
                    ROUND(AVG(health_score), 0) AS avg_health_score,
                    SUM(critical_count) AS total_criticals,
                    SUM(warning_count) AS total_warnings
                FROM kpis
                GROUP BY machine_id, machine_type
            """)
            if health:
                dashboard_cache["health"] = health

            kpis = _run_sql(f"""
                SELECT machine_id, machine_type, sensor_name,
                    COUNT(*) AS total_readings,
                    COUNT(CASE WHEN anomaly_status = 'CRITICAL' THEN 1 END) AS critical_count,
                    COUNT(CASE WHEN anomaly_status = 'WARNING' THEN 1 END) AS warning_count,
                    ROUND(AVG(value), 2) AS avg_value,
                    ROUND(MAX(value), 2) AS max_value,
                    ROUND(MIN(value), 2) AS min_value,
                    GREATEST(0, 100
                        - COUNT(CASE WHEN anomaly_status = 'CRITICAL' THEN 1 END) * 10
                        - COUNT(CASE WHEN anomaly_status = 'WARNING' THEN 1 END) * 3
                    ) AS health_score
                FROM {CATALOG}.{PIPELINE_SCHEMA}.enriched_events
                WHERE TRUE
                GROUP BY machine_id, machine_type, sensor_name
            """)
            if kpis:
                dashboard_cache["kpis"] = kpis

            anomalies = _run_sql(
                f"SELECT * FROM {CATALOG}.{PIPELINE_SCHEMA}.anomaly_timeline LIMIT 100"
            )
            dashboard_cache["anomalies"] = anomalies

            trends = _run_sql(f"""
                SELECT machine_id, sensor_name, value, unit, anomaly_status, timestamp
                FROM {CATALOG}.{PIPELINE_SCHEMA}.enriched_events
                WHERE timestamp > current_timestamp() - INTERVAL 30 MINUTES
                ORDER BY timestamp DESC
                LIMIT 500
            """)
            if trends:
                dashboard_cache["trends"] = trends

            logger.debug("Dashboard cache refreshed")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error(f"Dashboard cache refresh error: {e}")

        await asyncio.sleep(5)


# --- Dashboard Endpoints (serve from cache, instant) ---

@app.get("/api/dashboard/health")
async def dashboard_health():
    return dashboard_cache["health"]

@app.get("/api/dashboard/kpis")
async def dashboard_kpis():
    return dashboard_cache["kpis"]

@app.get("/api/dashboard/anomalies")
async def dashboard_anomalies():
    return dashboard_cache["anomalies"]

@app.get("/api/dashboard/trends")
async def dashboard_trends():
    return dashboard_cache["trends"]

@app.get("/api/dashboard/landing-count")
async def dashboard_landing_count():
    return {"count": 0}


# --- WebSocket ---

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        # Send initial state
        await websocket.send_json({
            "type": "init",
            "machines": get_machine_configs(),
            "fault_states": simulator.get_fault_states(),
        })
        # Keep connection alive, listen for client messages
        while True:
            data = await websocket.receive_text()
            msg = json.loads(data)
            if msg.get("action") == "inject_fault":
                simulator.inject_fault(msg["machine_id"])
            elif msg.get("action") == "clear_fault":
                simulator.clear_fault(msg["machine_id"])
            elif msg.get("action") == "clear_all":
                simulator.clear_all_faults()
    except WebSocketDisconnect:
        manager.disconnect(websocket)


# --- Serve React Frontend ---

FRONTEND_DIR = os.path.join(os.path.dirname(__file__), "..", "frontend", "dist")

if os.path.isdir(FRONTEND_DIR):
    app.mount("/assets", StaticFiles(directory=os.path.join(FRONTEND_DIR, "assets")), name="assets")

    @app.get("/{full_path:path}")
    async def serve_frontend(full_path: str):
        """Serve React SPA — all non-API routes return index.html."""
        file_path = os.path.join(FRONTEND_DIR, full_path)
        if os.path.isfile(file_path):
            return FileResponse(file_path)
        return FileResponse(os.path.join(FRONTEND_DIR, "index.html"))
