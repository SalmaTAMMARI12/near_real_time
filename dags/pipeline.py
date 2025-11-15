"""
DAG Airflow - Pipeline E-commerce Events
Orchestration complète : Vérifications → Spark Analytics → ClickHouse
Fréquence : Toutes les heures

NOTE: Producer et Consumer tournent en continu dans Docker.
Ce DAG fait uniquement le traitement batch horaire.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import logging
import psycopg2
from clickhouse_driver import Client
from confluent_kafka.admin import AdminClient

# --------------------------
# CONFIGURATION DU DAG
# --------------------------
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 10, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pipeline_orchestration_v2',
    default_args=default_args,
    description='Pipeline batch: Vérifications → Spark → ClickHouse',
    schedule_interval='@hourly',
    catchup=False,
    tags=['ecommerce', 'batch', 'spark', 'clickhouse'],
)

# ==============================================
# TASK 1 : Vérification de Kafka
# ==============================================
def check_kafka_health():
    try:
        admin_client = AdminClient({'bootstrap.servers': 'kafka-simple:9092'})
        metadata = admin_client.list_topics(timeout=10)
        topics = list(metadata.topics.keys())
        
        logging.info("=" * 60)
        logging.info("🔍 HEALTH CHECK - KAFKA")
        logging.info("=" * 60)
        logging.info(f"✅ Kafka opérationnel")
        logging.info(f"📋 Topics disponibles: {topics}")
        logging.info("=" * 60)
        
    except Exception as e:
        logging.error(f"❌ Kafka inaccessible: {e}")
        raise

check_kafka = PythonOperator(
    task_id='check_kafka_health',
    python_callable=check_kafka_health,
    dag=dag,
)

# ==============================================
# TASK 2 : Vérification de PostgreSQL
# ==============================================
def check_postgres_health():
    try:
        conn = psycopg2.connect(
            host='postgres-events',
            database='kafka_db',
            user='postgres',
            password='testpassword123'
        )
        
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.execute("""
                SELECT COUNT(*) 
                FROM events 
                WHERE created_at >= NOW() - INTERVAL '1 hour'
            """)
            recent_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM events")
            total_count = cur.fetchone()[0]
        
        conn.close()
        
        logging.info("=" * 60)
        logging.info("🔍 HEALTH CHECK - POSTGRESQL")
        logging.info("=" * 60)
        logging.info(f"✅ PostgreSQL opérationnel")
        logging.info(f"📊 Événements (1h): {recent_count:,}")
        logging.info(f"📊 Événements (total): {total_count:,}")
        
        if recent_count == 0:
            logging.warning("⚠️  Aucun événement dans la dernière heure!")
        
        logging.info("=" * 60)
        
    except Exception as e:
        logging.error(f"❌ PostgreSQL inaccessible: {e}")
        raise

check_postgres = PythonOperator(
    task_id='check_postgres_health',
    python_callable=check_postgres_health,
    dag=dag,
)

# ==============================================
# TASK 3 : Vérification de ClickHouse
# ==============================================
def check_clickhouse_health():
    try:
        client = Client(
            host='clickhouse',
            port=9000,
            user='default',
            password='monMotDePasse123'
        )
        version = client.execute('SELECT version()')[0][0]
        
        logging.info("=" * 60)
        logging.info("🔍 HEALTH CHECK - CLICKHOUSE")
        logging.info("=" * 60)
        logging.info(f"✅ ClickHouse opérationnel")
        logging.info(f"📦 Version: {version}")
        logging.info("=" * 60)
        
    except Exception as e:
        logging.error(f"❌ ClickHouse inaccessible: {e}")
        raise

check_clickhouse = PythonOperator(
    task_id='check_clickhouse_health',
    python_callable=check_clickhouse_health,
    dag=dag,
)

# ==============================================
# TASK 4 : Calcul des métriques horaires
# ==============================================
def compute_hourly_metrics():
    logging.info("🔹 Début du calcul des métriques horaires")
    try:
        conn = psycopg2.connect(
            host='postgres-events',
            database='kafka_db',
            user='postgres',
            password='testpassword123'
        )
        
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    event_type,
                    COUNT(*) as total_events,
                    AVG(request_latency_ms) as avg_latency,
                    COUNT(CASE WHEN status = 'ERROR' THEN 1 END) as error_count,
                    COUNT(CASE WHEN status = 'ERROR' THEN 1 END) * 100.0 / COUNT(*) as error_rate
                FROM events
                WHERE event_timestamp >= NOW() - INTERVAL '1 hour'
                GROUP BY event_type
                ORDER BY total_events DESC
            """)
            metrics = cur.fetchall()
        
        conn.close()
        
        logging.info("=" * 80)
        logging.info("📊 MÉTRIQUES HORAIRES")
        logging.info("=" * 80)
        
        for event_type, total, avg_lat, errors, error_rate in metrics:
            logging.info(f"""
📈 {event_type}:
   └─ Total: {total:,} événements
   └─ Latence moy: {avg_lat:.0f}ms
   └─ Erreurs: {errors} ({error_rate:.2f}%)
            """.strip())
        
        logging.info("=" * 80)
        
    except Exception as e:
        logging.error(f"❌ Erreur calcul métriques: {e}")
        raise

compute_metrics_task = PythonOperator(
    task_id='compute_hourly_metrics',
    python_callable=compute_hourly_metrics,
    dag=dag,
)

# ==============================================
# TASK 5 : Spark Batch Job
# ==============================================
run_spark_job = BashOperator(
    task_id='run_spark_analytics',
    bash_command="""
    echo "🔥 Lancement du job Spark..."
    docker exec spark /opt/spark/bin/spark-submit \
      --master spark://spark:7077 \
      --deploy-mode client \
      --executor-memory 2g \
      --driver-memory 1g \
      --jars /opt/spark/work-dir/jars/postgresql-42.6.0.jar,/opt/spark/work-dir/jars/clickhouse-jdbc-0.3.2-patch1-all.jar \
      /opt/spark/work-dir/scripts/spark_processing.py
    echo "✅ Job Spark terminé"
    """,
    dag=dag,
)

# ==============================================
# TASK 6 : Vérification des métriques ClickHouse
# ==============================================
def verify_clickhouse_metrics():
    try:
        client = Client(
            host='clickhouse',
            port=9000,
            user='default',
            password='monMotDePasse123'
        )
        tables = [t[0] for t in client.execute("SHOW TABLES")]
        
        logging.info("=" * 60)
        logging.info("🔍 VÉRIFICATION CLICKHOUSE")
        logging.info("=" * 60)
        logging.info(f"📋 Tables disponibles: {tables}")
        
        if 'event_metrics' in tables:
            count = client.execute('SELECT COUNT(*) FROM event_metrics')[0][0]
            logging.info(f"📊 Métriques stockées: {count:,} lignes")
        else:
            logging.warning("⚠️  Table 'event_metrics' non trouvée")
        
        logging.info("=" * 60)
        
    except Exception as e:
        logging.error(f"❌ Erreur ClickHouse: {e}")
        raise

verify_clickhouse = PythonOperator(
    task_id='verify_clickhouse_metrics',
    python_callable=verify_clickhouse_metrics,
    dag=dag,
)

# ==============================================
# TASK 7 : Notification de succès
# ==============================================
def send_success_notification(**context):
    execution_date = context['execution_date']
    
    logging.info("=" * 80)
    logging.info("✅ PIPELINE COMPLÉTÉ AVEC SUCCÈS")
    logging.info("=" * 80)
    logging.info(f"📅 Exécution: {execution_date}")
    logging.info(f"🔄 Flux: Health Checks → Métriques → Spark → ClickHouse")
    logging.info(f"⏱️  Prochain run: {context['next_execution_date']}")
    logging.info("=" * 80)

success_notification = PythonOperator(
    task_id='pipeline_success',
    python_callable=send_success_notification,
    provide_context=True,
    dag=dag,
)

# ==============================================
# FLUX DU PIPELINE
# ==============================================
[check_kafka, check_postgres, check_clickhouse] >> compute_metrics_task
compute_metrics_task >> run_spark_job >> verify_clickhouse >> success_notification
