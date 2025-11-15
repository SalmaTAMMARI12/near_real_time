#!/bin/bash
set -e

echo "⏳ Attente de Kafka et PostgreSQL..."

KAFKA_HOST=$(echo $KAFKA_BOOTSTRAP_SERVERS | cut -d: -f1)
KAFKA_PORT=$(echo $KAFKA_BOOTSTRAP_SERVERS | cut -d: -f2)

# Attendre Kafka
echo "⏳ Attente de Kafka sur $KAFKA_HOST:$KAFKA_PORT..."
until nc -z $KAFKA_HOST $KAFKA_PORT; do
  echo "⏳ Kafka pas encore prêt, nouvelle tentative dans 5s..."
  sleep 5
done
echo "✅ Kafka est prêt !"

# Attendre PostgreSQL
echo "⏳ Attente de PostgreSQL sur $POSTGRES_HOST:$POSTGRES_PORT..."
until nc -z $POSTGRES_HOST $POSTGRES_PORT; do
  echo "⏳ PostgreSQL pas encore prêt, nouvelle tentative dans 5s..."
  sleep 5
done
echo "✅ PostgreSQL est prêt !"

echo "🚀 Démarrage du consumer..."
exec python /app/scripts/mycustmer.py