import argparse
from os.path import join

from pyspark.sql import SparkSession
from pyspark.sql import functions as f

# Function to aggregate tweet data by date and calculate metrics.
def get_tweet_conversas(df_tweet):
    return df_tweet.alias("tweet")\
        .groupBy(f.to_date("created_at").alias("created_date"))\  # Group by the date part of the "created_at" field.
        .agg(
            f.countDistinct("author_id").alias("n_tweets"),  # Count distinct authors (number of tweets).
            f.sum("like_count").alias("n_like"),  # Sum of likes.
            f.sum("quote_count").alias("n_quote"),  # Sum of quotes.
            f.sum("reply_count").alias("n_reply"),  # Sum of replies.
            f.sum("retweet_count").alias("n_retweet")  # Sum of retweets.
        ).withColumn("weekday", f.date_format("created_date", "E"))  # Add a column for the day of the week.

# Function to export a DataFrame to JSON format.
def export_json(df, dest):
    df.coalesce(1).write.mode("overwrite").json(dest)  # Write the DataFrame as a single JSON file, overwriting if it exists.

# Main function to process Twitter insights.
def twitter_insight(spark, src, dest, process_date):
    # Read tweet data from the source directory.
    df_tweet = spark.read.json(join(src, 'tweet'))

    # Aggregate tweet data using the `get_tweet_conversas` function.
    tweet_conversas = get_tweet_conversas(df_tweet)

    # Export the aggregated data to the destination directory.
    export_json(tweet_conversas, join(dest, f"process_date={process_date}"))

# Execution block when the script is run directly.
if __name__ == "__main__":
    # Set up argument parsing for command-line inputs.
    parser = argparse.ArgumentParser(
        description="Spark Twitter Transformation Silver"
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

    # Call the `twitter_insight` function to process and export the data.
    twitter_insight(spark, args.src, args.dest, args.process_date)