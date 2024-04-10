#                ADMN5015 - Artificial Intelligence in Marketing
#
#        Marketing - Final Artificial Intelligence Project
#
#
#
#   Professor Milton Davila
#   Done by: Sri Venkatesh Subramaniam


# Project Title:
# Enhancing and facilitating Marketing Strategies and goals using Sentiment Analysis of Google Reviews
# for Tim Hortons in Kingston, Ontario


import os
import requests
import pandas as pd
import urllib.parse
from pprint import pprint
from dataclasses import dataclass
from dotenv import load_dotenv
from google.cloud import language_v1

from pprint import pprint

import requests
from bs4 import BeautifulSoup

import psycopg2
from psycopg2 import OperationalError

from firebase import insert_into_firestore

load_dotenv()


# Dataclass for reviews
@dataclass
class review_value:
    review_id: int
    displayName: str
    rating: float
    publishTime: str
    sentiment_score: float
    sentiment_magnitude: float
    address: str
    city: str = "Kingston"
    province: str = "Ontario"


# Dataclass for moderation values
@dataclass
class moderate_value:
    review_id: int
    toxic: int
    insult: int
    derogatory: int
    public_safety: int
    health: int
    finance: int


# Dataclass for text of reviews
@dataclass
class review_text:
    review_id: int
    text: str


def get_locations(url):
    """
    Take the url of tims location site,
    Scrapes the location,
    returns the locations[]
    """

    response = requests.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text)
        spans = soup.find_all("span", {"class": "sb-directory-sites-address"})
        # span class="sb-directory-sites-address"
        locations = [span.get_text() for span in spans]
    else:
        print("Failed to retrieve the webpage")
        raise Exception("Failed to retrieve the webpage")

    return locations


def get_geo_code(place):
    """
    Takes in the place,
    GET request GCP,
    return geo_code response
    """
    KEY = os.getenv("KEY")
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    params = {"address": place, "key": KEY}
    response = requests.get(url, params=urllib.parse.urlencode(params))

    return response.json()


def get_reviews(place_id):
    """
    Takes in the place_id
    GET Requests GCP
    returns the data.
    """

    KEY = os.getenv("KEY")
    url = "https://places.googleapis.com/v1/places/{}".format(place_id)
    params = {
        "fields": "id,displayName,rating,reviews",
        "key": KEY,
    }
    response = requests.get(url, params=urllib.parse.urlencode(params))
    return response.json()


def get_sentiment(review):
    """
    Takes the review > gets the sentiment score and magnitude from GCP
    returns the sentiment score and magnitude
    """
    # Set the environment variable
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service-account.json"

    # Instantiates a client
    client = language_v1.LanguageServiceClient()

    # Analyzing the reviews
    text = review

    document = language_v1.types.Document(
        content=text, type_=language_v1.types.Document.Type.PLAIN_TEXT
    )

    # Detects the sentiment of the text
    sentiment = client.analyze_sentiment(
        request={"document": document}
    ).document_sentiment

    return sentiment.score, sentiment.magnitude


def get_moderate(text, review_id: int):
    """
    Takes the review text and review_id
    gets the moderation values
    return the required value as the dataclass
    """

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "service-account.json"

    client = language_v1.LanguageServiceClient()
    # Initialize request argument(s)
    document = language_v1.types.Document(
        content=text, type_=language_v1.types.Document.Type.PLAIN_TEXT
    )

    request = language_v1.ModerateTextRequest(
        document=document,
    )

    moderated = client.moderate_text(request=request)
    # pprint(moderated)

    # our required value and their index in list:
    # toxic 0
    # Insult 1
    # derogatory 3
    # public safety 8
    # Health 9
    # finance 14
    category_indices = [0, 1, 3, 8, 9, 14]
    confidence_values = [
        moderated.moderation_categories[i].confidence for i in category_indices
    ]
    confidence_values.insert(0, review_id)
    values = moderate_value(*confidence_values)
    return values


def insert_into_df(values):
    """
    Takes the list of dataclass values
    makes a pandas dataframe of those values
    """
    df = pd.DataFrame([x.__dict__ for x in values])
    return df


def save_df(df, file_path):
    """Takes the df and file_path
    saves the df to that filepath
    """
    df.to_csv(file_path, index=False, sep="\t")
    print(f"Saved df -> {file_path}")


def create_connection(db_name, db_user, db_password, db_host, db_port):
    """Takes in the dbname, user, password, hots and port
    to Create db Connection
    return the Connection
    """
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection


def insert_into_db(df, connection):
    """
    Takes the df and connection
    makes the table
    and inserts df into PostgreSQL
    """

    cursor = connection.cursor()

    # Query for Creating the table
    create_table_query = """
    CREATE TABLE IF NOT EXISTS reviews (
    review_id NUMERIC,
    displayName VARCHAR,
    rating REAL,
    publishTime VARCHAR,
    sentiment_score REAL,
    sentiment_magnitude REAL,
    address VARCHAR,
    city VARCHAR,
    province VARCHAR);    
    """
    cursor.execute(create_table_query)
    connection.commit()

    # Save the dataframe to disk
    tmp_df = "./tmp_dataframe.tsv"
    df.to_csv(tmp_df, header=False, sep="\t", index=False)
    with open(tmp_df, "r", encoding="utf-8") as f:
        try:
            cursor.copy_from(f, "reviews", sep="\t")
            connection.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            os.remove(tmp_df)
            print("Error: %s" % error)
            connection.rollback()
            cursor.close()
            return 1
    print("copy_from_file() done")
    cursor.close()
    os.remove(tmp_df)


def insert_moderate_value_into_postgresql(df, connection):
    """
    Takes moderation value df and connection
    creates table in pgsql
    inserts the df into the table
    """

    cursor = connection.cursor()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS moderate (
    review_id NUMERIC,
    toxix NUMERIC,
    insult NUMERIC,
    derogatory NUMERIC,
    public_safety NUMERIC,
    health NUMERIC,
    finance NUMERIC)
    """
    cursor.execute(create_table_query)
    connection.commit()
    tmp_df = "./tmp_moderate_df.tsv"
    df.to_csv(tmp_df, header=False, sep="\t", index=False)
    with open(tmp_df, "r", encoding="utf-8") as f:
        try:
            cursor.copy_from(f, "moderate", sep="\t")
            connection.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            os.remove(tmp_df)
            print("Error: %s" % error)
            connection.rollback()
            cursor.close()
            return 1
    print("copy_from_file() done")
    cursor.close()
    os.remove(tmp_df)


def main():
    url = "https://locations.timhortons.ca/en/locations-list/on/kingston/"

    locations = get_locations(url)

    values = []
    reviews_list = []
    moderate_value_list = []

    # # for each location, gets placeid

    review_id = 100
    for place in locations:
        geo_code = get_geo_code(f"Tim Hortons, {place}")
        place_id = geo_code["results"][0]["place_id"]
        place_details = get_reviews(place_id)
        # pprint(place_details)

        # for each review get sentiment score and magnitude
        # appends each value to values list
        try:
            for i in range(len(place_details["reviews"])):

                text = place_details["reviews"][i]["originalText"]["text"]

                sentiment_score, sentiment_magnitude = get_sentiment(text)

                values.append(
                    review_value(
                        review_id=review_id,
                        displayName=place_details["reviews"][i]["authorAttribution"][
                            "displayName"
                        ],
                        rating=place_details["reviews"][i]["rating"],
                        publishTime=place_details["reviews"][i]["publishTime"],
                        sentiment_score=sentiment_score,
                        sentiment_magnitude=sentiment_magnitude,
                        address=f"Tim Hortons, {place}",
                    )
                )
                reviews_list.append(review_text(review_id, text=text))
                moderate_value_list.append(get_moderate(text, review_id))
                review_id = review_id + 1
        except Exception as e:
            print(f"Exception Occured for place_id: {place_id} : {e}")

    # Insert all reviews and data into a pandas dataframe
    df = insert_into_df(values)
    review_text_df = insert_into_df(reviews_list)
    moderate_value_df = insert_into_df(moderate_value_list)
    # save the df as a TSV file
    save_df(df, file_path="./reviews.tsv")
    save_df(review_text_df, file_path="./reviews_text.tsv")
    save_df(moderate_value_df, file_path="./moderate_value_df.tsv")

    df = pd.read_csv("./reviews.tsv", sep="\t")

    password = os.getenv("PASS")
    connection = create_connection("sectest", "admn5015", password, "localhost", "5432")
    insert_into_db(df, connection)

    # Inserting reviews_list(review_id, text) into firestore
    insert_into_firestore(reviews_list, collection="tim_hortons")

    insert_moderate_value_into_postgresql(moderate_value_df, connection)


if __name__ == "__main__":
    main()
