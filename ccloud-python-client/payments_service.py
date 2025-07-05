from confluent_kafka import Consumer, Producer
import random

def create_producer(config):
    producer_config = {
        k: v for k, v in config.items()
        if k not in ["group.id", "auto.offset.reset"]
    }
    return Producer(producer_config)

def read_config():
    config = {}
    with open("client.properties") as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split("=", 1)
                config[parameter] = value.strip()
    return config

def produce(topic, producer, key, value):
    try:
        producer.produce(topic, key=key.encode("utf-8"), value=value.encode("utf-8"))
        producer.flush()  # ensure message is sent before continuing
        print(f"✅ Produced to {topic}: key={key} | value={value}")
    except Exception as e:
        print(f"❌ Failed to produce to {topic}: {e}")

def consume(config):
    config["group.id"] = "python-group-1"
    config["auto.offset.reset"] = "earliest"

    consumer = Consumer(config)
    producer = create_producer(config)

    consumer.subscribe(["order_topic"])
    print("🚀 Payment processor started. Waiting for orders...\n")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"⚠️ Consumer error: {msg.error()}")
                continue

            key = msg.key().decode("utf-8") if msg.key() else "unknown"
            value = msg.value().decode("utf-8") if msg.value() else ""

            print(f"💳 Processing payment: key = {key} | value = {value}")

            if random.randint(1, 2) == 1:
                produce("payment_success_topic", producer, key, value)
            else:
                produce("payment_failure_topic", producer, key, value)

    except KeyboardInterrupt:
        print("\n🛑 Stopping payment processor...")
    finally:
        consumer.close()
        print("🔌 Consumer connection closed.")

def main():
    config = read_config()
    consume(config)

main()
