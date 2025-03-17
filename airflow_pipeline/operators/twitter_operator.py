import sys
sys.path.append("airflow_pipeline")

from airflow.models import BaseOperator, DAG, TaskInstance
from hook.twitter_hook import TwitterHook
import json
from datetime import datetime, timedelta
from os.path import join
from pathlib import Path

# Define a custom operator called TwitterOperator that inherits from BaseOperator.
class TwitterOperator(BaseOperator):

    # Define template fields that can be templated in Airflow.
    template_fields = ["query", "file_path", "start_time", "end_time"]

    # Initialize the operator with required parameters.
    def __init__(self, file_path, end_time, start_time, query, **kwargs):
        self.end_time = end_time  # End time for the Twitter query.
        self.start_time = start_time  # Start time for the Twitter query.
        self.query = query  # Search term for tweets.
        self.file_path = file_path  # Path where the extracted tweets will be saved.
        super().__init__(**kwargs)  # Initialize the parent class (BaseOperator).

    # Method to create the parent folder for the output file if it doesn't exist.
    def create_parent_folder(self):
        (Path(self.file_path).parent).mkdir(parents=True, exist_ok=True)  # Create the folder structure.

    # Method that executes the main logic of the operator.
    def execute(self, context):
        end_time = self.end_time  # End time for the query.
        start_time = self.start_time  # Start time for the query.
        query = self.query  # Search term.

        # Ensure the parent folder exists.
        self.create_parent_folder()

        # Open the output file in write mode and save the tweets as JSON.
        with open(self.file_path, "w") as output_file:
            for pg in TwitterHook(end_time, start_time, query).run():  # Use TwitterHook to fetch tweets.
                json.dump(pg, output_file, ensure_ascii=False)  # Write the JSON data to the file.
                output_file.write("\n")  # Add a newline after each JSON object.

# Execution block when the script is run directly.
if __name__ == "__main__":
    # Define the timestamp format used in the Twitter API.
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

    # Set the end time to the current moment.
    end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
    # Set the start time to 1 day before the current moment.
    start_time = (datetime.now() + timedelta(-1)).date().strftime(TIMESTAMP_FORMAT)
    # Define the search query.
    query = "datascience"

    # Create a DAG for testing purposes.
    with DAG(dag_id="TwitterTest", start_date=datetime.now()) as dag:
        # Create an instance of the TwitterOperator.
        to = TwitterOperator(
            file_path=join(
                "datalake/twitter_datascience",  # Base folder for the datalake.
                f"extract_date={datetime.now().date()}",  # Folder for the extraction date.
                f"datascience_{datetime.now().date().strftime('%Y%m%d')}.json"  # Output file name.
            ),
            query=query,  # Search query.
            start_time=start_time,  # Start time for the query.
            end_time=end_time,  # End time for the query.
            task_id="test_run"  # Task ID.
        )
        # Create a TaskInstance for the operator.
        ti = TaskInstance(task=to)
        # Execute the operator.
        to.execute(ti.task_id)