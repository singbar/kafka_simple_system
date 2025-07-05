from confluent_kafka import Producer, Consumer

def read_config():
    config = {}
    with open("client.properties") as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split("=", 1)
                config[parameter] = value.strip()
    return config

def consume(config):
    # Set consumer-specific configs
    config["group.id"] = "python-group-1"
    config["auto.offset.reset"] = "earliest"

    consumer = Consumer(config)
    consumer.subscribe(["^.*"])

    print(f"📥 Subscribed to all topics' — waiting for messages (Ctrl+C to stop)...\n")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"⚠️  Consumer error: {msg.error()}")
                continue

            key = msg.key().decode("utf-8") if msg.key() else None
            value = msg.value().decode("utf-8") if msg.value() else None
            print(f"✅ Consumed | Topic: {msg.topic()} | Partition: {msg.partition()} | Offset: {msg.offset()}")
            print(f"   Key: {key} | Value: {value}\n")
    except KeyboardInterrupt:
        print("\n🛑 Consumer interrupted by user.")
    finally:
        consumer.close()
        print("🔌 Consumer connection closed.")

def main():
    config = read_config()
    consume(config)

main()
