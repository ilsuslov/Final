# shop_api.py - эмуляция SHOP API с TLS
# Читает товары из файла и отправляет в Kafka через SSL

import json
import time
from confluent_kafka import Producer


def delivery_report(err, msg):
    """Callback для подтверждения доставки"""
    if err:
        print(f"❌ Ошибка: {err}")


def main():
    # Конфигурация с TLS
    conf = {
        'bootstrap.servers': 'localhost:9092,localhost:9093',
        'security.protocol': 'SSL',
        'ssl.ca.location': '../ssl/ca.crt',
        'ssl.certificate.location': '../ssl/client.crt',
        'ssl.key.location': '../ssl/client.key',
        'ssl.endpoint.identification.algorithm': '',
        'client.id': 'shop-api'
    }
    
    producer = Producer(conf)
    
    with open('../data/products.json', 'r', encoding='utf-8') as f:
        products = json.load(f)
    
    print(f"🚀 [Shop API] Загружено {len(products)} товаров")
    print("🔐 Подключение через TLS...")
    
    for product in products:
        producer.produce(
            'products-raw',
            key=product['product_id'],
            value=json.dumps(product, ensure_ascii=False),
            callback=delivery_report
        )
        producer.flush()
        print(f"📦 Отправлен: {product['name']}")
        time.sleep(1)
    
    print("✅ [Shop API] Все товары отправлены")


if __name__ == "__main__":
    main()
