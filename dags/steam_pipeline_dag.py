from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

# Generate timestamped run_id for all tasks
def generate_run_id():
    return datetime.now().strftime("%Y-%m-%dT%H-%M-%S")

# Each wrapper runs the corresponding script with the same run_id
def run_clean_data(run_id):
    subprocess.run(["python", "scripts/clean_data.py", "--run_id", run_id], check=True)

def run_join_games(run_id):
    subprocess.run(["python", "scripts/join_games.py", "--run_id", run_id], check=True)

def run_generate_game_features(run_id):
    subprocess.run(["python", "scripts/generate_game_features.py", "--run_id", run_id], check=True)

def run_generate_user_features(run_id):
    subprocess.run(["python", "scripts/generate_user_features.py", "--run_id", run_id], check=True)

# Airflow DAG definition
default_args = {
    "owner": "azma",
    "start_date": datetime(2024, 1, 1),  # must be in the past
    "retries": 0,
}

with DAG(
    dag_id="steam_game_pipeline",
    default_args=default_args,
    schedule_interval=None,  # manual trigger
    catchup=False,
    tags=["steam", "recommender", "pipeline"],
) as dag:

    run_id = generate_run_id()

    t1 = PythonOperator(
        task_id="clean_data",
        python_callable=run_clean_data,
        op_args=[run_id],
    )

    t2 = PythonOperator(
        task_id="join_games",
        python_callable=run_join_games,
        op_args=[run_id],
    )

    t3 = PythonOperator(
        task_id="generate_game_features",
        python_callable=run_generate_game_features,
        op_args=[run_id],
    )

    t4 = PythonOperator(
        task_id="generate_user_features",
        python_callable=run_generate_user_features,
        op_args=[run_id],
    )

    # Task dependencies
    t1 >> t2 >> [t3, t4]
s