from datetime import datetime, timedelta
import requests
import json
import os

# Define the timestamp format used in the Twitter API.
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

# Set the end time to the current moment.
end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
# Set the start time to 1 day before the current moment.
start_time = (datetime.now() + timedelta(-1)).date().strftime(TIMESTAMP_FORMAT)
# Define the search query.
query = "datascience"

# Define the fields to be returned for tweets.
tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text"
# Define the fields to be returned for users.
user_fields = "expansions=author_id&user.fields=id,name,username,created_at"

# Build the URL for querying the Twitter API.
url_raw = f"https://labdados.com/2/tweets/search/recent?query={query}&{tweet_fields}&{user_fields}&start_time={start_time}&end_time={end_time}"

# Set up the headers for the API request, including the bearer token for authentication.
bearer_token = os.environ.get("BEARER_TOKEN")  # Retrieve the bearer token from environment variables.
headers = {"Authorization": "Bearer {}".format(bearer_token)}  # Add the token to the headers.

# Make the initial request to the Twitter API.
response = requests.request("GET", url_raw, headers=headers)

# Parse the JSON response and print it in a readable format.
json_response = response.json()
print(json.dumps(json_response, indent=4, sort_keys=True))

# Paginate through the results if there is a "next_token" in the response metadata.
while "next_token" in json_response.get("meta", {}):
    next_token = json_response['meta']['next_token']  # Get the token for the next page.
    url = f"{url_raw}&next_token={next_token}"  # Build the URL for the next page.
    response = requests.request("GET", url, headers=headers)  # Make the request for the next page.
    json_response = response.json()  # Parse the JSON response.
    print(json.dumps(json_response, indent=4, sort_keys=True))  # Print the JSON response in a readable format.