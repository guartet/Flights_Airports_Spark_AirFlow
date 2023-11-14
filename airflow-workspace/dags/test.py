from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Définissez vos tâches
def task_1():
    print("Exécution de la tâche 1")

def task_2():
    print("Exécution de la tâche 2")

def task_3():
    print("Exécution de la tâche 3")

# Définissez les arguments de votre DAG
default_args = {
    'owner': 'votre_nom',
    'start_date': datetime(2023, 11, 7),
    'retries': 1,
}

# Créez votre DAG
dag = DAG(
    'test',
    default_args=default_args,
    schedule="0 0 * * *",  # Choisissez l'intervalle approprié ="0 0 * * *"
)


# Créez les opérateurs pour chaque tâche
task_1 = PythonOperator(
    task_id='task_1',
    python_callable=task_1,
    dag=dag,
)

task_2 = PythonOperator(
    task_id='task_2',
    python_callable=task_2,
    dag=dag,
)

task_3 = PythonOperator(
    task_id='task_3',
    python_callable=task_3,
    dag=dag,
)

# Définissez l'ordre d'exécution des tâches
task_1 >> task_2 >> task_3
