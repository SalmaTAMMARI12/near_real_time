#!/bin/bash
set -e

echo "⏳ Attente de Kafka sur $KAFKA_BOOTSTRAP_SERVERS..."

KAFKA_HOST=$(echo $KAFKA_BOOTSTRAP_SERVERS | cut -d: -f1)
KAFKA_PORT=$(echo $KAFKA_BOOTSTRAP_SERVERS | cut -d: -f2)

until nc -z $KAFKA_HOST $KAFKA_PORT; do
  echo "⏳ Kafka pas encore prêt, nouvelle tentative dans 5s..."
  sleep 5
done

echo "✅ Kafka est prêt !"
echo "🚀 Démarrage du producer..."
exec python /app/scripts/myproducer.py