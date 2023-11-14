


from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess as sb



def prepare_data():
    sb.run(['python', '.../Script/Data_preparation.py'])

def train_model():
    sb.run(['python', '../Script/ML_model.py'])
    
def visualisation():
    sb.run(['python3', '../scripts/visualisation.py'])






default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 11, 13),
    'retries': 1,
}

dag = DAG(
    'Take_Exam',
    default_args=default_args,
    schedule="0 0 * * *",  # Choisissez l'intervalle approprié ="0 0 * * *"
)

# Task 1: Ingestion and Data Preparation
ingestion_task = PythonOperator(
    task_id='data_ingestion_preparation',
    python_callable=prepare_data,
    dag=dag,
)
# Task 1: Visualisation
visualisation_task = PythonOperator(
    task_id='data_visualisation',
    python_callable=prepare_data,
    dag=dag,
)

# Task 3: ML Model Training
ml_training_task = PythonOperator(
    task_id='train_ml_model',
    python_callable=train_model,
    dag=dag,
)

ingestion_task >> visualisation_task >> ml_training_task
