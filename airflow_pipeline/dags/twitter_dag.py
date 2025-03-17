import sys
sys.path.append("airflow_pipeline")

from airflow.models import DAG
from datetime import datetime, timedelta
from operators.twitter_operator import TwitterOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from os.path import join
from airflow.utils.dates import days_ago
from pathlib import Path

# Defines a DAG (Directed Acyclic Graph) in Apache Airflow with the ID "TwitterDAG".
# The DAG starts execution 6 days ago and has a daily schedule.
with DAG(dag_id="TwitterDAG", start_date=days_ago(6), schedule_interval="@daily") as dag:

    # Defines the base path to store data in the datalake.
    # The path is composed of a base folder, a stage (Bronze, Silver, Gold), and a partition.
    BASE_FOLDER = join(
        str(Path("~/Documents").expanduser()),  # Expands the user's path to the Documents folder.
        "curso2/datalake/{stage}/twitter_datascience/{partition}",  # Folder structure of the datalake.
    )

    # Defines the partition folder for data extraction, using the start date of the execution interval.
    PARTITION_FOLDER_EXTRACT = "extract_date={{ data_interval_start.strftime('%Y-%m-%d') }}"

    # Defines the timestamp format used in Twitter queries.
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

    # Defines the query that will be used to search for tweets related to "datascience".
    query = "datascience"

    # Creates a task using the custom TwitterOperator.
    # This operator is responsible for extracting tweets based on the query and the specified time range.
    twitter_operator = TwitterOperator(
        file_path=join(
            BASE_FOLDER.format(stage="Bronze", partition=PARTITION_FOLDER_EXTRACT),  # Path where raw data will be saved.
            "datascience_{{ ds_nodash }}.json"  # Name of the JSON file that will store the tweets.
        ),
        query=query,  # Query used to search for tweets.
        start_time="{{ data_interval_start.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",  # Start time of the search interval.
        end_time="{{ data_interval_end.strftime('%Y-%m-%dT%H:%M:%S.00Z') }}",  # End time of the search interval.
        task_id="twitter_datascience"  # Task ID.
    )

    # Creates a task using the SparkSubmitOperator to transform raw data (Bronze) into processed data (Silver).
    twitter_transform = SparkSubmitOperator(
        task_id="transform_twitter_datascience",  # Task ID.
        application="/home/aluno/Documents/curso2/src/spark/transformation.py",  # Path to the Spark transformation script.
        name="twitter_transformation",  # Name of the Spark application.
        application_args=[
            "--src", BASE_FOLDER.format(stage="Bronze", partition=PARTITION_FOLDER_EXTRACT),  # Source path for raw data.
            "--dest", BASE_FOLDER.format(stage="Silver", partition=""),  # Destination path for processed data.
            "--process-date", "{{ ds }}"  # Processing date.
        ]
    )

    # Creates a task using the SparkSubmitOperator to generate insights from the processed data (Silver).
    twitter_insight = SparkSubmitOperator(
        task_id="insight_twitter",  # Task ID.
        application="/home/aluno/Documents/curso2/src/spark/insight_tweet.py",  # Path to the Spark insight generation script.
        name="insight_twitter",  # Name of the Spark application.
        application_args=[
            "--src", BASE_FOLDER.format(stage="Silver", partition=""),  # Source path for processed data.
            "--dest", BASE_FOLDER.format(stage="Gold", partition=""),  # Destination path for data with insights.
            "--process-date", "{{ ds }}"  # Processing date.
        ]
    )

    # Defines the order of task execution:
    # 1. Tweet extraction (twitter_operator).
    # 2. Data transformation (twitter_transform).
    # 3. Insight generation (twitter_insight).
    twitter_operator >> twitter_transform >> twitter_insight