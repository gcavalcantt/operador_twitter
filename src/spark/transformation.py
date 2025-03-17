from pyspark.sql import functions as f
from pyspark.sql import SparkSession

from os.path import join
import argparse

# Function to extract and transform tweet data from the raw JSON.
def get_tweets_data(df):
    tweet_df = df.select(f.explode("data").alias("tweets"))\  # Explode the "data" array to get individual tweets.
                .select(
                    "tweets.author_id",  # Select the author ID.
                    "tweets.conversation_id",  # Select the conversation ID.
                    "tweets.created_at",  # Select the creation timestamp.
                    "tweets.id",  # Select the tweet ID.
                    "tweets.public_metrics.*",  # Select all public metrics (likes, retweets, etc.).
                    "tweets.text"  # Select the tweet text.
                )
    return tweet_df

# Function to extract and transform user data from the raw JSON.
def get_users_data(df):
    user_df = df.select(f.explode("includes.users").alias("users"))\  # Explode the "includes.users" array to get individual users.
                .select("users.*")  # Select all user fields.
    return user_df

# Function to export a DataFrame to JSON format.
def export_json(df, dest):
    df.coalesce(1).write.mode("overwrite").json(dest)  # Write the DataFrame as a single JSON file, overwriting if it exists.

# Main function to process and transform Twitter data.
def twitter_transformation(spark, src, dest, process_date):
    # Read the raw JSON data from the source directory.
    df = spark.read.json(src)

    # Extract and transform tweet data.
    tweet_df = get_tweets_data(df)
    # Extract and transform user data.
    user_df = get_users_data(df)

    # Define the destination path for the output, partitioned by table name and processing date.
    table_dest = join(dest, "{table_name}", f"process_date={process_date}")

    # Export tweet data to JSON.
    export_json(tweet_df, table_dest.format(table_name="tweet"))
    # Export user data to JSON.
    export_json(user_df, table_dest.format(table_name="user"))

# Execution block when the script is run directly.
if __name__ == "__main__":
    # Set up argument parsing for command-line inputs.
    parser = argparse.ArgumentParser(
        description="Spark Twitter Transformation"
    )
    parser.add_argument("--src", required=True)  # Source directory for raw data.
    parser.add_argument("--dest", required=True)  # Destination directory for processed data.
    parser.add_argument("--process-date", required=True)  # Processing date for partitioning.
    args = parser.parse_args()

    # Initialize a Spark session.
    spark = SparkSession\
        .builder\
        .appName("twitter_transformation")\  # Set the application name.
        .getOrCreate()

    # Call the `twitter_transformation` function to process and export the data.
    twitter_transformation(spark, args.src, args.dest, args.process_date)