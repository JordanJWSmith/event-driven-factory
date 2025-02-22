# send payload to main ingest
curl -X POST http://127.0.0.1:8000/ingest \
-H "Content-Type: application/json" \
-d '{"machine_id": "value", "temperature": 33.2, "vibration": 10.0}'


# list available kafka topics
docker exec kafka kafka-topics.sh --list --bootstrap-server localhost:9092

# create a topic
docker exec kafka kafka-topics.sh --create --topic cnc_sensor_data --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
