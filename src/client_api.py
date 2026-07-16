# client_api.py - CLIENT API с Elasticsearch
# Поиск товаров в Elasticsearch, рекомендации из Kafka

import json
import requests
from confluent_kafka import Consumer, Producer


ES_URL = "http://localhost:9200"
INDEX_NAME = "products"


def search_in_elasticsearch(query):
    """Поиск товара в Elasticsearch"""
    try:
        response = requests.get(f"{ES_URL}/{INDEX_NAME}/_search", json={
            "query": {
                "multi_match": {
                    "query": query,
                    "fields": ["name", "description", "category"]
                }
            }
        })
        
        if response.status_code == 200:
            data = response.json()
            hits = data['hits']['hits']
            if hits:
                print(f"\n🔍 Найдено {len(hits)} результатов:")
                for hit in hits:
                    source = hit['_source']
                    print(f"  - {source.get('name', 'Unknown')} ({source.get('category', '')})")
                    print(f"    Цена: {source.get('price', {}).get('amount', 0)} {source.get('price', {}).get('currency', '')}")
            else:
                print("⚠️  Ничего не найдено")
        else:
            print(f"❌ Ошибка Elasticsearch: {response.status_code}")
    except Exception as e:
        print(f" Ошибка подключения к Elasticsearch: {e}")


def index_product(product):
    """Индексировать товар в Elasticsearch"""
    try:
        response = requests.post(
            f"{ES_URL}/{INDEX_NAME}/_doc/{product['product_id']}",
            json=product,
            headers={'Content-Type': 'application/json'}
        )
        if response.status_code in [200, 201]:
            print(f"✅ Индексирован: {product['name']}")
        else:
            print(f"️  Ошибка индексации: {response.status_code}")
    except Exception as e:
        print(f"❌ Ошибка: {e}")


def main():
    print("🛒 [Client API] Запуск...")
    
    # Consumer для рекомендаций
    consumer = Consumer({
        'bootstrap.servers': 'localhost:9095',
        'group.id': 'client-api-group',
        'auto.offset.reset': 'latest',
        'security.protocol': 'SSL',
        'ssl.ca.location': '../ssl/ca.crt',
        'ssl.endpoint.identification.algorithm': ''
    })
    consumer.subscribe(['recommendations'])
    
    # Producer для событий поиска
    producer = Producer({
        'bootstrap.servers': 'localhost:9095',
        'security.protocol': 'SSL',
        'ssl.ca.location': '../ssl/ca.crt',
        'ssl.endpoint.identification.algorithm': ''
    })
    
    print("✅ Подключен к Kafka и Elasticsearch")
    
    while True:
        print("\n" + "="*50)
        print("МЕНЮ КЛИЕНТА")
        print("="*50)
        print("1. Поиск товара (Elasticsearch)")
        print("2. Получить рекомендацию (Kafka)")
        print("3. Выйти")
        print("="*50)
        
        choice = input("\nВаш выбор (1/2/3): ").strip()
        
        if choice == '1':
            query = input("Введите запрос: ").strip()
            print(f"\n🔍 Поиск в Elasticsearch: '{query}'...")
            search_in_elasticsearch(query)
            
            # Отправляем событие поиска в Kafka для аналитики
            producer.produce('user-events', value=json.dumps({
                'event': 'search',
                'query': query,
                'timestamp': '2026-07-16T10:00:00Z'
            }).encode())
            producer.flush()
        
        elif choice == '2':
            print("\n⏳ Загрузка рекомендаций...")
            msg = consumer.poll(timeout=2.0)
            
            if msg is None:
                print("️  Рекомендаций пока нет")
            elif msg.error():
                print(f"❌ Ошибка: {msg.error()}")
            else:
                rec = json.loads(msg.value().decode('utf-8'))
                print(f"\n⭐ ВАМ МОЖЕТ ПОНРАВИТЬСЯ:")
                print(f"   Товар: {rec['product_name']}")
                print(f"   Совпадение: {rec['score'] * 100:.0f}%")
                print(f"   Причина: {rec['reason']}")
        
        elif choice == '3':
            print("\n👋 До свидания!")
            break
        
        else:
            print("❌ Неверный выбор")
    
    consumer.close()


if __name__ == "__main__":
    main()
