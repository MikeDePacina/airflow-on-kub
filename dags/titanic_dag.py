from airflow.decorators import task, dag
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import requests
import pandas as pd

default_args = {
    'owner': 'YouKnowMe',
    'start_date': datetime(2021, 1, 1),
}

@dag(
    default_args=default_args,
    schedule_interval='@once',
    catchup=False, # Don't run several pending tasks at once
    description="Simple pipeline with titanic data",
    tags=['titanic']
)
def titanic_processing():
    #using DummyOperator to set markers in DAG, doesn't do anything
    start = DummyOperator(task_id='start')
    @task
    def first_task():
        print('First task is running')
    
    @task
    def download_data():
        destination = "/tmp/titanic.csv"
        response = requests.get(
            "https://raw.githubusercontent.com/neylsoncrepalde/titanic_data_with_semicolon/main/titanic.csv",
            stream=True
        )
        with open(destination, mode='wb') as file:
            file.write(response.content)
        return destination
    
    @task
    def analyze_survivors(source):
        df = pd.read_csv(source, sep=';')
        res = df.loc[df.Survived == 1, "Survived"].sum()
        print(res)
    
    @task
    def survivors_sex(source):
        df = pd.read_csv(source, sep=';')
        res = df.loc[df.Survived == 1, ["Survived", "Sex"]].groupby("Sex").count()
        print(res)
    
    last = BashOperator(
        task_id="last_task",
        bash_command='echo "This is the last task performed with Bash."',
    )

    end = DummyOperator(task_id='end')

    first = first_task()
    downloaded = download_data()
    start >> first >> downloaded
    surv_count = analyze_survivors(downloaded) #output of downloaded
    surv_sex = survivors_sex(downloaded) #output of downloaded
    [surv_count, surv_sex] >> last >> end #in list means they can run in parallel

execution = titanic_processing()
