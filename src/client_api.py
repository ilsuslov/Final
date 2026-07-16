import json
from confluent_kafka import Consumer

def main():
    print("🛒 [Client API] Запущен.")
    consumer = Consumer({'bootstrap.servers': 'localhost:9094', 'group.id': 'client-group', 'auto.offset.reset': 'latest'})
    consumer.subscribe(['recommendations'])

    while True:
        print("\n--- МЕНЮ КЛИЕНТА ---")
        print("1. Получить персональную рекомендацию")
        print("2. Выход")
        choice = input("Ваш выбор (1/2): ").strip()
        
        if choice == '1':
            print("⏳ Загрузка рекомендаций из Kafka...")
            msg = consumer.poll(2.0)
            if msg and not msg.error():
                rec = json.loads(msg.value().decode())
                print(f"⭐ ВАМ МОЖЕТ ПОНРАВИТЬСЯ: {rec['product_name']}")
                print(f"   Совпадение: {rec['score'] * 100}% | Причина: {rec['reason']}")
            else:
                print("⚠️ Пока нет новых рекомендаций. Убедитесь, что запущен analytics_worker.py")
        elif choice == '2':
            print("👋 До свидания!")
            break
        else:
            print("❌ Неверный выбор.")
            
    consumer.close()

if __name__ == "__main__":
    main()
