import time, uuid, random, logging, subprocess
from multiprocessing import Process
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from flask import json
from config_simple import (
    KAFKA_BOOTSTRAP_SERVERS, 
    KAFKA_TOPIC, 
    MAX_EVENTS, 
    NUM_WORKERS, 
    EVENT_INTERVAL_SECONDS, 
    NEW_USER_SESSION_PROBABILITY,
    Events,
    Status
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Vérifier que le topic existe
def ensure_topic_exists():
    admin_client = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    topics = admin_client.list_topics(timeout=10).topics
    if KAFKA_TOPIC not in topics:
        logging.info(f"Topic {KAFKA_TOPIC} introuvable, création...")
        admin_client.create_topics([NewTopic(KAFKA_TOPIC, num_partitions=1, replication_factor=1)])
    else:
        logging.info(f"Topic {KAFKA_TOPIC} existe déjà.")

def generate_event(user_id: str, session_id: str):
    has_error = random.random() < 0.1
    event_type = random.choice(list(Events)).value
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "session_id": session_id,
        "event_type": event_type,
        "event_timestamp": int(time.time() * 1000),
        "request_latency_ms": random.randint(50, 1500),
        "status": (Status.ERROR.value if has_error else Status.SUCCESS.value),
        "error_code": random.randint(400, 599) if has_error else None,
        "product_id": random.randint(1, 10000) if event_type in {Events.VIEW_PRODUCT, Events.ADD_TO_CART} else None
    }

def delivery_report(err, msg):
    if err:
        logging.error(f"❌ Message delivery failed: {err}")
    else:
        logging.debug(f"✅ Message delivered to {msg.topic()} [{msg.partition()}]")

def worker(worker_id: int):
    producer_config = {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS, "client.id": f"producer-worker-{worker_id}"}
    producer = Producer(producer_config)
    user_id, session_id = str(uuid.uuid4()), str(uuid.uuid4())
    event_count = 0
    while True:
        try:
            event = generate_event(user_id, session_id)
            producer.produce(KAFKA_TOPIC, key=user_id, value=json.dumps(event), callback=delivery_report)
            producer.poll(0)
            event_count += 1
            if event_count % 100 == 0:
                logging.info(f"📊 Worker {worker_id}: {event_count} events produits")
            if random.random() < NEW_USER_SESSION_PROBABILITY:
                user_id, session_id = str(uuid.uuid4()), str(uuid.uuid4())
            time.sleep(EVENT_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            break
        except Exception as e:
            logging.error(f"❌ Worker {worker_id} erreur: {e}")
            time.sleep(1)
    producer.flush()
    logging.info(f"🛑 Worker {worker_id} arrêté ({event_count} events)")

if __name__ == "__main__":
    logging.info("="*50)
    logging.info(f"🎬 Kafka Producer démarrage, serveur: {KAFKA_BOOTSTRAP_SERVERS}")
    logging.info("="*50)
    # Retry pour s'assurer que Kafka est prêt
    for i in range(5):
        try:
            ensure_topic_exists()
            break
        except Exception as e:
            logging.warning(f"⏳ Kafka pas prêt, retry dans 5s... ({e})")
            time.sleep(5)
    processes = [Process(target=worker, args=(i,)) for i in range(NUM_WORKERS)]
    for p in processes: p.start()
    try:
        for p in processes: p.join()
    except KeyboardInterrupt:
        logging.info("⏸️ Arrêt des workers...")
        for p in processes: p.terminate()
        for p in processes: p.join()
        logging.info("✅ Tous les workers arrêtés")
