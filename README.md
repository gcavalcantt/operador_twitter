# Twitter Data Pipeline with Apache Airflow and Spark

This project is a data pipeline that extracts, transforms, and analyzes Twitter data using **Apache Airflow** for orchestration and **Apache Spark** for data processing. The pipeline is designed to fetch tweets related to a specific query (e.g., "datascience"), process the data, and generate insights such as engagement metrics (likes, retweets, etc.). The processed data is stored in a structured format for further analysis.

### Features

- **Data Extraction**:
  - Fetches tweets from the Twitter API using a custom Airflow operator (`TwitterOperator`).
  - Supports pagination to retrieve large datasets.
  - Saves raw tweet data in JSON format.

- **Data Transformation**:
  - Processes raw tweet data using Apache Spark.
  - Extracts relevant fields such as `author_id`, `created_at`, `text`, and engagement metrics (likes, retweets, etc.).
  - Aggregates data by date and calculates metrics like the number of tweets, likes, quotes, replies, and retweets.

- **Data Insights**:
  - Generates insights from processed data, such as daily engagement metrics.
  - Stores insights in a structured format for further analysis.

- **Orchestration**:
  - Uses Apache Airflow to orchestrate the pipeline, ensuring tasks like extraction, transformation, and insight generation are executed in the correct order.

- **Scalability**:
  - Built on Apache Spark, the pipeline can handle large volumes of data efficiently.
  - Output data is partitioned by date for easy querying and management.

### How It Works

1. **Data Extraction**:
   - The `TwitterOperator` fetches tweets from the Twitter API based on a query (e.g., "datascience") and a time range.
   - Raw tweet data is saved in JSON format in a specified directory (e.g., `datalake/bronze`).

2. **Data Transformation**:
   - The `twitter_transformation.py` script processes raw tweet data using Spark.
   - It extracts relevant fields and calculates engagement metrics.
   - Processed data is saved in a structured format (e.g., `datalake/silver`).

3. **Data Insights**:
   - The `twitter_insight.py` script generates insights from the processed data, such as daily engagement metrics.
   - Insights are saved in a structured format (e.g., `datalake/gold`).

4. **Orchestration**:
   - Apache Airflow orchestrates the pipeline, ensuring tasks are executed in the correct order.
   - The DAG (`twitter_dag.py`) defines the workflow, including extraction, transformation, and insight generation.