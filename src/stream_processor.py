# stream_processor.py - Faust с TLS
# Фильтрует запрещенные товары

import faust
import json

app = faust.App(
    'stream-processor',
    broker='kafka://localhost:9092',
    value_serializer='raw',
    # TLS конфигурация для Faust
    broker_credentials=faust.SSL(
        cafile='../ssl/ca.crt',
        certfile='../ssl/client.crt',
        keyfile='../ssl/client.key'
    )
)

with open('../data/banned.json', 'r', encoding='utf-8') as f:
    BANNED_CATEGORIES = set(json.load(f))

print(f"🛡️ [Stream Processor] Запрещенные категории: {BANNED_CATEGORIES}")

products_raw = app.topic('products-raw')
products_filtered = app.topic('products-filtered')


@app.agent(products_raw)
async def process_products(products):
    async for product_bytes in products:
        product = json.loads(product_bytes.decode('utf-8'))
        category = product.get('category', '')
        name = product.get('name', 'Unknown')
        
        if category in BANNED_CATEGORIES:
            print(f"🚫 [Faust] ЗАБЛОКИРОВАНО: {name} ({category})")
        else:
            print(f"✅ [Faust] ПРОПУЩЕНО: {name} -> products-filtered")
            await products_filtered.send(value=product_bytes)


if __name__ == '__main__':
    print("🛡️ [Stream Processor] Запуск Faust с TLS...")
    app.main()
