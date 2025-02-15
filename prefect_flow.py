# prefect_flow.py
from prefect import flow, task
import subprocess

@task(name="Executar Bronze Pipeline")
def run_bronze_pipeline():
    """Executa o script Python do Bronze Pipeline."""
    try:
        subprocess.run(["python", "brewery_data_pipeline/bronze_pipeline.py"], check=True, capture_output=True)
        print("Bronze Pipeline executado com sucesso.")
    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar Bronze Pipeline: {e.stderr.decode()}")
        raise

@task(name="Executar Silver Pipeline")
def run_silver_pipeline():
    """Executa o script Python do Silver Pipeline."""
    try:
        subprocess.run(["python", "brewery_data_pipeline/silver_pipeline.py"], check=True, capture_output=True)
        print("Silver Pipeline executado com sucesso.")
    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar Silver Pipeline: {e.stderr.decode()}")
        raise

@task(name="Executar Gold Pipeline")
def run_gold_pipeline():
    """Executa o script Python do Gold Pipeline."""
    try:
        subprocess.run(["python", "brewery_data_pipeline/gold_pipeline.py"], check=True, capture_output=True)
        print("Gold Pipeline executado com sucesso.")
    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar Gold Pipeline: {e.stderr.decode()}")
        raise

@flow(name="Pipeline de Cervejarias Medallion")
def brewery_pipeline_flow():
    """Flow principal para orquestrar o pipeline medallion de cervejarias."""
    run_bronze_pipeline_task = run_bronze_pipeline()
    run_silver_pipeline_task = run_silver_pipeline(wait_for=[run_bronze_pipeline_task]) # Silver depende do Bronze
    run_gold_pipeline_task = run_gold_pipeline(wait_for=[run_silver_pipeline_task])   # Gold depende do Silver

if __name__ == "__main__":
    brewery_pipeline_flow() # Executa o flow localmente para teste