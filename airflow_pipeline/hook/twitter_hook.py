from airflow.providers.http.hooks.http import HttpHook
import requests
from datetime import datetime, timedelta
import json

# Define a custom class TwitterHook that inherits from HttpHook.
class TwitterHook(HttpHook):

    # Class initialization method.
    def __init__(self, end_time, start_time, query, conn_id=None):
        self.end_time = end_time  # Defines the end time for the query.
        self.start_time = start_time  # Defines the start time for the query.
        self.query = query  # Defines the search term (query) for tweets.
        self.conn_id = conn_id or "twitter_default"  # Sets the default connection or uses the provided one.
        super().__init__(http_conn_id=self.conn_id)  # Initializes the base HttpHook class.

    # Method to create the URL for querying the Twitter API.
    def create_url(self):
        TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"  # Timestamp format used in the URL.

        end_time = self.end_time  # End time for the query.
        start_time = self.start_time  # Start time for the query.
        query = self.query  # Search term.

        # Fields for tweets that will be returned by the API.
        tweet_fields = "tweet.fields=author_id,conversation_id,created_at,id,in_reply_to_user_id,public_metrics,lang,text"
        # Fields for users that will be returned by the API.
        user_fields = "expansions=author_id&user.fields=id,name,username,created_at"

        # Builds the complete URL for querying the Twitter API.
        url_raw = f"{self.base_url}/2/tweets/search/recent?query={query}&{tweet_fields}&{user_fields}&start_time={start_time}&end_time={end_time}"

        return url_raw  # Returns the constructed URL.

    # Method to connect to the API endpoint and make the request.
    def connect_to_endpoint(self, url, session):
        request = requests.Request("GET", url)  # Creates a GET request.
        prep = session.prepare_request(request)  # Prepares the request.
        self.log.info(f"URL: {url}")  # Logs the URL being accessed.
        return self.run_and_check(session, prep, {})  # Executes the request and returns the response.

    # Method to paginate the results from the API query.
    def paginate(self, url_raw, session):
        lista_json_response = []  # List to store JSON responses.
        response = self.connect_to_endpoint(url_raw, session)  # Makes the first request.
        json_response = response.json()  # Converts the response to JSON.
        lista_json_response.append(json_response)  # Adds the response to the list.
        contador = 1  # Counter to limit the number of pages.

        # Loop to paginate results while there is a "next_token" in the response.
        while "next_token" in json_response.get("meta", {}) and contador < 10:
            next_token = json_response['meta']['next_token']  # Gets the token for the next page.
            url = f"{url_raw}&next_token={next_token}"  # Builds the URL for the next page.
            response = self.connect_to_endpoint(url, session)  # Makes the request for the next page.
            json_response = response.json()  # Converts the response to JSON.
            lista_json_response.append(json_response)  # Adds the response to the list.
            contador += 1  # Increments the counter.

        return lista_json_response  # Returns the list with all responses.

    # Main method that executes the class logic.
    def run(self):
        session = self.get_conn()  # Gets an HTTP session.
        url_raw = self.create_url()  # Creates the URL for the query.
        return self.paginate(url_raw, session)  # Returns the paginated results.

# Execution block when the script is run directly.
if __name__ == "__main__":
    TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"  # Timestamp format.

    # Sets the end time as the current moment.
    end_time = datetime.now().strftime(TIMESTAMP_FORMAT)
    # Sets the start time as 1 day before the current moment.
    start_time = (datetime.now() + timedelta(-1)).date().strftime(TIMESTAMP_FORMAT)
    query = "datascience"  # Defines the search query.

    # Executes the TwitterHook class and prints the formatted JSON responses.
    for pg in TwitterHook(end_time, start_time, query).run():
        print(json.dumps(pg, indent=4, sort_keys=True))  # Prints the JSON in an organized format.