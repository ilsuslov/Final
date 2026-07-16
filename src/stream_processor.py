import json
from confluent_kafka import Consumer, Producer

def main():
    with open('../data/banned.json', 'r', encoding='utf-8') as f:
        banned = set(json.load(f))

    consumer = Consumer({'bootstrap.servers': 'localhost:9092', 'group.id': 'filter-group', 'auto.offset.reset': 'earliest'})
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    consumer.subscribe(['products-raw'])

    print(f"🛡️ [Stream Processor] Запущен. Категории в черном списке: {banned}")
    try:
        while True:
            msg = consumer.poll(1.0)
            if not msg or msg.error(): continue
            data = json.loads(msg.value().decode('utf-8'))
            
            if data.get('category') in banned:
                print(f"🚫 [Stream Processor] ЗАБЛОКИРОВАНО: {data['name']} (Категория: {data['category']})")
            else:
                print(f"✅ [Stream Processor] ПРОПУЩЕНО: {data['name']} -> products-filtered")
                producer.produce('products-filtered', key=msg.key(), value=msg.value())
                producer.flush()
    except KeyboardInterrupt:
        print("\n⏹️ [Stream Processor] Остановлен.")
        consumer.close()

if __name__ == "__main__":
    main()
