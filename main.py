from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from pydantic import BaseModel
from consumer import process_message
from prometheus_fastapi_instrumentator import Instrumentator # type: ignore
from contextlib import asynccontextmanager
import logging
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer # type: ignore
import asyncio
import json
import sqlite3
import psycopg2

logger = logging.getLogger(__name__)

KAFKA_TOPIC = "cnc_sensor_data"
KAFKA_BOOTSTRAP_SERVER = "localhost:9092"

DB_CONFIG = {
    "dbname": "sensor_data",
    "user": "admin",
    "password": "admin",
    "host": "localhost",
    "port": "5432"
}

active_connections = set()

def setup_database():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sensor_data (
            id SERIAL PRIMARY KEY,
            machine_id TEXT,
            temperature DOUBLE PRECISION
            vibration DOUBLE PRECISION,
            timestamp TIMESTAMPTZ DEFAULT NOW()        
        );
    """)
    conn.commit()
    conn.close()


@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_database()
    task = asyncio.create_task(kafka_consumer())
    yield
    task.cancel()

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


async def kafka_consumer():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER
    )
    await consumer.start()

    try:
        async for msg in consumer:
            data = json.loads(msg.value)
            for conn in active_connections:
                await conn.send_json(data)

    finally:
        await consumer.stop()


@app.get("/history/{machine_id}")
async def get_machine_data(machine_id: str):
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT machine_id, temperature, vibration, timestamp
        FROM sensor_data
        WHERE machine_id = %s
        ORDER BY timestamp DESC
        LIMIT 100
    """, (machine_id,)
    )
    data = cursor.fetchall()
    conn.close()

    return {"data": data}

# class SensorData(BaseModel):
#     machine_id: str
#     temperature: float
#     vibration: float


# @app.post("/ingest")
# async def ingest_data(data: SensorData):
#     print(f"ingest endpoint received: {data.dict()}")
#     message = json.dumps(data.dict()).encode("utf-8")
#     await producer.send(KAFKA_TOPIC, message)
#     return {"status": "data received"}


