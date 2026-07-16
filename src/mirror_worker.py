import json
from confluent_kafka import Consumer, Producer

def main():
    print("🔄 [Mirror Worker] Репликация данных из Кластера 1 (9092) в Кластер 2 (9094)...")
    consumer = Consumer({'bootstrap.servers': 'localhost:9092', 'group.id': 'mirror-group', 'auto.offset.reset': 'earliest'})
    producer = Producer({'bootstrap.servers': 'localhost:9094'})
    consumer.subscribe(['products-filtered'])

    try:
        while True:
            msg = consumer.poll(1.0)
            if not msg or msg.error(): continue
            data = json.loads(msg.value().decode('utf-8'))
            producer.produce('products-filtered-replicated', key=msg.key(), value=msg.value())
            producer.flush()
            print(f"📥 [Mirror Worker] Реплицировано в Кластер 2: {data['name']}")
    except KeyboardInterrupt:
        print("\n⏹️ [Mirror Worker] Остановлен.")
        consumer.close()

if __name__ == "__main__":
    main()
