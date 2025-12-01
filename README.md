# near_real_time

## 🚀 Description du projet

**near_real_time** est un pipeline de données en quasi-temps réel, permettant d’ingérer, traiter et stocker des données en continu.
Il est conçu pour des cas comme l’ingestion d’événements, le monitoring de flux, ou l’alimentation de dashboards analytiques.
Le projet intègre également Airflow pour l’orchestration des tâches et Metabase pour la visualisation des données.

---

## 🧩 Architecture du projet

<img width="1436" height="591" alt="archi_proje_near" src="https://github.com/user-attachments/assets/c8ea0f9e-54ee-48a2-b4a4-fb27056d8b9a" />


- **Producer** : Génère ou récupère les données et les envoie au broker.  
- **Broker (Kafka)** : File de messages pour bufferiser et transmettre les données.  
- **Consumer** : Consomme les messages, transforme les données et les stocke dans la base.  
- **Base de données (ClickHouse)** : Stockage analytique pour permettre des requêtes rapides.
- **Airflow (DAGs)** : Orchestration et planification des tâches pour automatiser le pipeline.
- **Metabase** : Tableau de bord pour visualiser les données ingérées et analysées.

---

## 📂 Structure du projet

<img width="485" height="539" alt="image" src="https://github.com/user-attachments/assets/58c61a2f-9ef5-4ceb-a9a7-499dfceb586a" />
near_real_time/
├── clickhouse_data/       # Configurations ou données ClickHouse
├── dags/                  # Orchestration / planification de tâches Airflow
├── jars/                  # Librairies Java
├── scripts/               # Scripts utilitaires (initialisation, ingestion…)
├── docker-compose.yml     # Orchestration Docker des services
├── dockerfile.producer    # Dockerfile du producteur
├── dockerfile.consumer    # Dockerfile du consommateur
├── requirements.txt       # Dépendances Python
├── wait-for-kafka.sh      # Script pour attendre Kafka
├── wait-for-services.sh   # Script pour attendre tous les services
└── metrics.json           # Définition des métriques

![WhatsApp Image 2025-10-10 à 00 36 42_42f4f620](https://github.com/user-attachments/assets/73f445d9-9c12-46f4-a804-461e30379875)
![WhatsApp Image 2025-10-10 à 00 42 09_dbca6eb4](https://github.com/user-attachments/assets/bb94a936-1fd3-4c3a-840f-978044ee5bf4)
![WhatsApp Image 2025-10-10 à 00 42 36_f100a0b6](https://github.com/user-attachments/assets/82c8b978-81b3-4946-9164-2f2293f6ac0d)
![WhatsApp Image 2025-10-10 à 00 47 03_cd83a268](https://github.com/user-attachments/assets/d1fc6085-9f74-473d-b4b3-fa3cd7d58660)




---

## 🛠 Prérequis

- Docker et Docker Compose
- Python (si utilisation des scripts Python)
- Ports libres pour Kafka, ClickHouse, Airflow et Metabase
- Accès au navigateur pour Metabase


---

## 🚀 Installation et exécution

1. Cloner le dépôt :  
```bash
git clone https://github.com/SalmaTAMMARI12/near_real_time.git
cd near_real_time
2. Construire et lancer tous les services avec Docker Compose
Construire et lancer les services
docker-compose up --build
Les scripts **wait-for-kafka.sh** et **wait-for-services.sh** garantissent que tous les services sont prêts avant de démarrer les producers et consumers.
