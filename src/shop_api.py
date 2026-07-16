import json, time
from confluent_kafka import Producer

def main():
    producer = Producer({'bootstrap.servers': 'localhost:9092'})
    with open('../data/products.json', 'r', encoding='utf-8') as f:
        products = json.load(f)

    print("🚀 [Shop API] Загружено товаров:", len(products))
    for p in products:
        producer.produce('products-raw', key=p['product_id'], value=json.dumps(p, ensure_ascii=False))
        producer.flush()
        print(f"📦 Отправлен: {p['name']}")
        time.sleep(1)
    print("✅ [Shop API] Все товары успешно отправлены в Kafka.")

if __name__ == "__main__":
    main()
