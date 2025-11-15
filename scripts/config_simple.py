# config_simple.py
POSTGRES_CONN = {
    "host": "postgres",
    "database": "kafka_db",
    "user": "postgres",
    "password": "testpassword123",
    "port": 5432
}
# Kafka
KAFKA_BOOTSTRAP_SERVERS = "kafka-simple:9092"
KAFKA_TOPIC = "user-events"

# Workers
NUM_WORKERS = 2             # nombre de workers pour le producer
MAX_EVENTS = 1000           # nombre total d'événements à produire
EVENT_INTERVAL_SECONDS = 0.01  # intervalle entre chaque événement
NEW_USER_SESSION_PROBABILITY = 0.05  # probabilité de démarrer une nouvelle session utilisateur

# Événements possibles
from enum import Enum

class Events(Enum):
    VIEW_PRODUCT = "view_product"
    ADD_TO_CART = "add_to_cart"
    PURCHASE = "purchase"

class Status(Enum):
    SUCCESS = "SUCCESS"
    ERROR = "ERROR"
