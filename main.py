from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from consumer import process_message
from prometheus_fastapi_instrumentator import Instrumentator # type: ignore
from contextlib import asynccontextmanager
import logging
from aiokafka import AIOKafkaProducer # type: ignore
import asyncio
import json
import sqlite3

logger = logging.getLogger(__name__)

KAFKA_TOPIC = "cnc_sensor_data"
KAFKA_BOOTSTRAP_SERVER = "localhost:9092"

active_connections = set()

conn = sqlite3.connect("sensor_data.db", check_same_thread=False)
cursor = conn.cursor()
init_sql = "CREATE TABLE IF NOT EXISTS sensor_data (id INTEGER PRIMARY KEY, machine_id TEXT, temperature REAL, vibration REAL, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP)"
cursor.execute(init_sql)
conn.commit()

producer = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER)
    await producer.start()
    logger.info("Kafka Producer started ✅")

    try: 
        yield
    finally:
        await producer.stop()
        logger.info("Kafka Producer stopped ❌")

app = FastAPI(lifespan=lifespan)
Instrumentator().instrument(app).expose(app)

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_connections.add(websocket)

    try:
        while True:
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        active_connections.remove(websocket)

class SensorData(BaseModel):
    machine_id: str
    temperature: float
    vibration: float


@app.post("/ingest")
async def ingest_data(data: SensorData):
    print(f"ingest endpoint received: {data.dict()}")
    message = json.dumps(data.dict()).encode("utf-8")
    await producer.send(KAFKA_TOPIC, message)
    return {"status": "data received"}


