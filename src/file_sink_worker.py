import json
from confluent_kafka import Consumer

def main():
    print("💾 [File Sink] Запись очищенных данных в data/output.txt...")
    consumer = Consumer({'bootstrap.servers': 'localhost:9094', 'group.id': 'filesink-group', 'auto.offset.reset': 'earliest'})
    consumer.subscribe(['products-filtered-replicated'])

    with open('../data/output.txt', 'w', encoding='utf-8') as f:
        try:
            while True:
                msg = consumer.poll(1.0)
                if not msg or msg.error(): continue
                data = json.loads(msg.value().decode('utf-8'))
                f.write(json.dumps(data, ensure_ascii=False) + '\n')
                f.flush()
                print(f"💾 [File Sink] Сохранено в файл: {data['name']}")
        except KeyboardInterrupt:
            print("\n⏹️ [File Sink] Остановлен.")
            consumer.close()

if __name__ == "__main__":
    main()
