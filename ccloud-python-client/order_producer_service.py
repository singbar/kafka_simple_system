from confluent_kafka import Producer
import json
import random
import time

# Load product catalog
with open("products.json", "r") as f:
    products = json.load(f)

def read_config():
    config = {}
    with open("client.properties") as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                config[parameter] = value.strip()
    return config

def produce(topic, config):
    producer = Producer(config)

    product = random.choice(products)
    key = product["sku"].encode("utf-8")  # Must be bytes
    value = f"Price = {product['price']}; Name = {product['name']}".encode("utf-8")

    producer.produce(topic, key=key, value=value)
    print(f"Produced message to topic {topic}: key = {key.decode()} | value = {value.decode()}")

    producer.flush()

def main():
    config = read_config()
    topic = "order_topic"
    for _ in range(10):
        produce(topic, config)
        time.sleep(random.uniform(1, 5))  # Wait randomly between 1-5 seconds

main()
