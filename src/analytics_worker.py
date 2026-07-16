import json
from confluent_kafka import Consumer, Producer

def main():
    print("📊 [Analytics Worker] Генерация рекомендаций на основе данных Кластера 2...")
    consumer = Consumer({'bootstrap.servers': 'localhost:9094', 'group.id': 'analytics-group', 'auto.offset.reset': 'earliest'})
    producer = Producer({'bootstrap.servers': 'localhost:9094'})
    consumer.subscribe(['products-filtered-replicated'])

    try:
        while True:
            msg = consumer.poll(1.0)
            if not msg or msg.error(): continue
            data = json.loads(msg.value().decode('utf-8'))
            
            # Простая логика аналитики: дешевые товары получают высокий скор
            score = 0.95 if data['price']['amount'] < 10000 else 0.60
            rec = {'user_id': 'demo_user', 'product_name': data['name'], 'score': score, 'reason': 'Популярно в вашей категории'}
            
            producer.produce('recommendations', value=json.dumps(rec, ensure_ascii=False))
            producer.flush()
            print(f"💡 [Analytics] Сгенерирована рекомендация: {data['name']} (Score: {score})")
    except KeyboardInterrupt:
        print("\n⏹️ [Analytics Worker] Остановлен.")
        consumer.close()

if __name__ == "__main__":
    main()
