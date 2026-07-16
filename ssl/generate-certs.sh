#!/bin/bash
# generate-certs.sh - генерация TLS сертификатов для Kafka

set -e

echo " Генерация TLS сертификатов..."

# Создаём директорию
mkdir -p ssl

# Генерируем CA
openssl req -new -x509 -keyout ssl/ca.key -out ssl/ca.crt -days 365 -nodes -subj "/CN=Kafka CA/O=Marketplace/C=RU"
echo "✅ CA сертификат создан"

# Генерируем сертификаты для каждого брокера
for broker in kafka-1 kafka-2 kafka-analytics; do
    echo " Генерация сертификата для $broker..."
    
    # Создаём конфиг с SAN
    cat > ssl/${broker}.cnf << EOF
[req]
req_extensions = v3_req
distinguished_name = req_distinguished_name

[req_distinguished_name]

[v3_req]
basicConstraints = CA:FALSE
keyUsage = nonRepudiation, digitalSignature, keyEncipherment
subjectAltName = @alt_names

[alt_names]
DNS.1 = $broker
DNS.2 = localhost
IP.1 = 127.0.0.1
EOF
    
    # Генерируем ключ и CSR
    openssl genrsa -out ssl/${broker}.key 2048
    openssl req -new -key ssl/${broker}.key -out ssl/${broker}.csr -subj "/CN=$broker/O=Marketplace/C=RU" -config ssl/${broker}.cnf
    
    # Подписываем сертификат CA
    openssl x509 -req -in ssl/${broker}.csr -CA ssl/ca.crt -CAkey ssl/ca.key -CAcreateserial -out ssl/${broker}.crt -days 365 -extensions v3_req -extfile ssl/${broker}.cnf
    
    echo "✅ Сертификат для $broker создан"
done

# Создаём keystore и truststore для Java (нужно для Kafka)
echo "☕ Создание Java keystore/truststore..."

# Truststore (содержит CA сертификат)
keytool -import -file ssl/ca.crt -alias ca -keystore ssl/truststore.jks -storepass changeit -noprompt

# Keystore для каждого брокера
for broker in kafka-1 kafka-2 kafka-analytics; do
    # Конвертируем в PKCS12
    openssl pkcs12 -export -in ssl/${broker}.crt -inkey ssl/${broker}.key -out ssl/${broker}.p12 -passout pass:changeit
    
    # Импортируем в JKS
    keytool -importkeystore -srckeystore ssl/${broker}.p12 -srcstoretype PKCS12 -srcstorepass changeit -destkeystore ssl/${broker}.keystore.jks -deststorepass changeit -noprompt
    
    # Добавляем CA в keystore брокера
    keytool -import -file ssl/ca.crt -alias ca -keystore ssl/${broker}.keystore.jks -storepass changeit -noprompt
    
    echo "✅ Keystore для $broker создан"
done

echo "🎉 Все сертификаты сгенерированы в папке ssl/"
ls -lh ssl/
