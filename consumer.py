import asyncio
import json
import sqlite3
import logging
from aiokafka import AIOKafkaConsumer #type: ignore

logger = logging.getLogger(__name__)

KAFKA_TOPIC = "cnc_sensor_data"
KAFKA_BOOTSTRAP_SERVER="localhost:9092"

# conn = sqlite3.connect("sensor_data.db", check_same_thread=False)
# cursor=conn.cursor()

async def process_message():
    consumer = AIOKafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        group_id="sensor_consumer_group",
        auto_offset_reset="earliest"
    )

    await consumer.start()
    logging.info("Kafka Consumer started ✅")
    try:
        async for msg in consumer:
            try:
                data = json.loads(msg.value)
                machine_id = data['machine_id']
                temperature = data['temperature']
                vibration = data['vibration']

                with sqlite3.connect("sensor_data.db") as conn:
                    cursor = conn.cursor()
                    insert_sql = "INSERT INTO sensor_data (machine_id, temperature, vibration) values (?, ?, ?)"
                    cursor.execute(insert_sql, (machine_id, temperature, vibration))
                    conn.commit()

                logger.info(f"✅ Inserted data: {data}")

                if temperature > 100 or vibration > 5.0:
                    logger.warning(f"⚠️ Alert: Machine {machine_id} is overheating or vibrating too much!")

            except json.JSONDecodeError as e:
                logger.error(f"Error decoding JSON: {e}")

            except sqlite3.DatabaseError as e:
                logger.error(f"SQLite error: {e}")

    except Exception as e:
        logger.critical(f"Kafka Consumer Error: {e}")

    finally:
        await consumer.stop()


# if __name__ == "__main__":
#     asyncio.run(process_message())