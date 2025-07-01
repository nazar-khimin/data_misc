
### 📹 Kafka Consumer/Producer with saving to Postqresql

## Commands:

Topic List
```
docker exec -it <container_id> kafka-topics.sh --bootstrap-server localhost:9094 --list
```

Create topic
```
docker exec -it <container_id> kafka-topics.sh --create --bootstrap-server localhost:9094 --replication-factor 1 --partitions 3 --topic data_stream
```

Delete kafka topic
```
kafka-topics --bootstrap-server localhost:9094 --delete --topic driven_data_stream
```

Test topic
```
docker exec -it <container_id> kafka-console-producer.sh --bootstrap-server localhost:9094 --topic data_stream
```

## Set up
1. Spin up env
```
docker compose up -d
docker compose down
```
2. Create PostqreSQL
3. Run [streaming_consumer.py](streaming_consumer.py) and [streaming_producer.py](streaming_producer.py)


