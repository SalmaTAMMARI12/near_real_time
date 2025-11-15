import psycopg2
from confluent_kafka import Consumer, KafkaException, KafkaError
import json
import logging
import time
from config_simple import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, POSTGRES_CONN

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Configuration du consumer
consumer_conf = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'group.id': 'consumer_pg_group',
    'auto.offset.reset': 'latest',
    'enable.auto.commit': False  # Commit manuel pour plus de contrôle
}

def create_table_if_not_exists(cursor):
    """Crée la table events si elle n'existe pas"""
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id SERIAL PRIMARY KEY,
            event_id VARCHAR(255),
            user_id VARCHAR(255),
            session_id VARCHAR(255),
            event_type VARCHAR(50),
            event_timestamp TIMESTAMP,
            request_latency_ms INTEGER,
            status VARCHAR(20),
            error_code INTEGER,
            product_id INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    logging.info("✅ Table 'events' vérifiée/créée")

def main():
    logging.info("=" * 50)
    logging.info("🎬 Démarrage du Kafka Consumer -> PostgreSQL")
    logging.info("=" * 50)
    logging.info(f"📡 Kafka servers: {KAFKA_BOOTSTRAP_SERVERS}")
    logging.info(f"📝 Kafka topic: {KAFKA_TOPIC}")
    logging.info(f"🗄️  PostgreSQL host: {POSTGRES_CONN['host']}")
    logging.info("=" * 50)
    
    # Connexion PostgreSQL
    max_retries = 5
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(**POSTGRES_CONN)
            cursor = conn.cursor()
            create_table_if_not_exists(cursor)
            conn.commit()
            logging.info("✅ Connecté à PostgreSQL")
            break
        except Exception as e:
            if attempt < max_retries - 1:
                logging.warning(f"⚠️  Tentative {attempt + 1}/{max_retries} échouée: {e}")
                logging.info(f"⏳ Nouvelle tentative dans {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                logging.error(f"❌ Impossible de se connecter à PostgreSQL après {max_retries} tentatives")
                raise
    
    # Connexion Kafka
    consumer = Consumer(consumer_conf)
    consumer.subscribe([KAFKA_TOPIC])
    logging.info(f"✅ Abonné au topic: {KAFKA_TOPIC}")
    
    event_count = 0
    batch_size = 10
    batch = []
    
    try:
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            
            # Décodage JSON
            try:
                event = json.loads(msg.value().decode("utf-8"))
                batch.append(event)
                
                # Insertion par batch pour meilleures performances
                if len(batch) >= batch_size:
                    insert_batch(cursor, conn, batch)
                    event_count += len(batch)
                    batch = []
                    consumer.commit()
                    
                    if event_count % 100 == 0:
                        logging.info(f"📊 {event_count} événements insérés en base")
                        
            except json.JSONDecodeError as e:
                logging.error(f"❌ Erreur de décodage JSON: {e}")
            except Exception as e:
                logging.error(f"❌ Erreur lors de l'insertion: {e}")
                conn.rollback()
    
    except KeyboardInterrupt:
        logging.info("\n⏸️  Arrêt du consumer...")
        # Insérer les événements restants
        if batch:
            insert_batch(cursor, conn, batch)
            event_count += len(batch)
        logging.info(f"✅ Total: {event_count} événements insérés")
    
    finally:
        consumer.close()
        cursor.close()
        conn.close()
        logging.info("🛑 Consumer arrêté proprement")

def insert_batch(cursor, conn, events):
    """Insère un batch d'événements en base"""
    for event in events:
        try:
            cursor.execute(
                """
                INSERT INTO events (
                    event_id, user_id, session_id, event_type, event_timestamp,
                    request_latency_ms, status, error_code, product_id
                )
                VALUES (%s, %s, %s, %s, to_timestamp(%s/1000.0), %s, %s, %s, %s)
                """,
                (
                    event.get("event_id"),
                    event.get("user_id"),
                    event.get("session_id"),
                    event.get("event_type"),
                    event.get("event_timestamp"),
                    event.get("request_latency_ms"),
                    event.get("status"),
                    event.get("error_code"),
                    event.get("product_id"),
                )
            )
        except Exception as e:
            logging.error(f"❌ Erreur insertion event {event.get('event_id')}: {e}")
    
    conn.commit()

if __name__ == "__main__":
    main()