from confluent_kafka import Consumer, KafkaError
from influxdb import InfluxDBClient
import json

TOPIC = "telemetry"

settings = {
    "bootstrap.servers": "my-cluster-kafka-brokers:9092",
    "group.id": "mygroup",
    # "client.id": "client-1",
    "enable.auto.commit": True,
    "session.timeout.ms": 6000,
    "default.topic.config": {"auto.offset.reset": "smallest"},
}

c = Consumer(settings)

c.subscribe([TOPIC])
client = InfluxDBClient(
    "vminsert-example-vmcluster-persistent", 8480, path="/insert/0/influx"
)


try:
    while True:
        msg = c.poll(0.1)
        if msg is None:
            continue
        elif not msg.error():
            # print("Received message: {0}".format(msg.value()))
            client.write_points([json.loads(msg.value().decode('utf-8'))])
        elif msg.error().code() == KafkaError._PARTITION_EOF:
            print(
                "End of partition reached {0}/{1}".format(msg.topic(), msg.partition())
            )
        else:
            print("Error occured: {0}".format(msg.error().str()))

except KeyboardInterrupt:
    pass

finally:
    c.close()
