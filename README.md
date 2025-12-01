# near_real_time

## 🚀 Description du projet

**near_real_time** est un pipeline de données en **quasi-temps réel**, permettant d’ingérer, traiter et stocker des données en continu.  
Il est conçu pour des cas comme l’ingestion d’événements, le monitoring de flux, ou l’alimentation de dashboards analytiques.

---

## 🧩 Architecture du projet

<img width="1436" height="591" alt="archi_proje_near" src="https://github.com/user-attachments/assets/c8ea0f9e-54ee-48a2-b4a4-fb27056d8b9a" />


- **Producer** : Génère ou récupère les données et les envoie au broker.  
- **Broker (Kafka)** : File de messages pour bufferiser et transmettre les données.  
- **Consumer** : Consomme les messages, transforme les données et les stocke dans la base.  
- **Base de données (ClickHouse)** : Stockage analytique pour permettre des requêtes rapides.

---

## 📂 Structure du projet

<img width="485" height="539" alt="image" src="https://github.com/user-attachments/assets/58c61a2f-9ef5-4ceb-a9a7-499dfceb586a" />
near_real_time/
├── clickhouse_data/ # Configurations ou données ClickHouse
├── dags/ # Orchestration / planification de tâches
├── jars/ # Librairies Java 
├── scripts/ # Scripts utilitaires
├── docker-compose.yml # Orchestration Docker des services
├── dockerfile.producer # Dockerfile du producteur
├── dockerfile.consumer # Dockerfile du consommateur
├── requirements.txt # Dépendances Python 
├── wait-for-kafka.sh # Script pour attendre Kafka
├── wait-for-services.sh # Script pour attendre tous les services
└── metrics.json # Définition des métriques

---

## 🛠 Prérequis

- Docker et Docker Compose  
- Python (si utilisation des scripts Python)  
- Ports libres pour Kafka, ClickHouse et autres services

---

## 🚀 Installation et exécution

1. Cloner le dépôt :  
```bash
git clone https://github.com/SalmaTAMMARI12/near_real_time.git
cd near_real_time

Construire et lancer les services
docker-compose up --build
