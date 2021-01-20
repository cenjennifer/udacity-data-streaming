from kafka import KafkaConsumer

consumer = KafkaConsumer(bootstrap_servers=["localhost:9092"],
                         auto_offset_reset="earliest",
                         request_timeout_ms=1000,
                         max_poll_records=10
                        )

topic=["com.udacity.spark.sf.policecalls"]
consumer.subscribe(topics=topic)

for message in consumer:
    print(message.value)