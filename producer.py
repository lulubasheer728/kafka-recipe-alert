import json
import logging

import requests
from recipe_scrapers import scrape_html
from kafka import KafkaProducer


def fetch_recipes():
    url = "https://www.allrecipes.com/recipe/232743/curry-chicken-salad-with-grapes/"

    html_response = requests.get(url, headers={"User-Agent": f"test"}).content
    scraper = scrape_html(html_response, org_url=url)
    response = scraper.to_json()

    return response


def connect_kafka_producer():
    try:
        producer = KafkaProducer(bootstrap_servers='localhost:9092')
        logging.info('Connected to kafka-producer successfully..')

        return producer
    except Exception as exception:
        raise Exception('Connection failed', exception)


def publish_message(producer_instance: KafkaProducer, topic_name: str, message_key: str, message_value: json):
    try:
        message_key_bytes = bytes(message_key, encoding='utf-8')
        message_value_bytes = message_value.encode('utf-8')
        producer_instance.send(topic_name, key=message_key_bytes, value=message_value_bytes)

        # block until all async messages are sent
        producer_instance.flush()

        logging.info(f'Message published to {topic_name} !!')
    except Exception as exception:
        logging.error(f"Couldn't publish message to {topic_name}: {exception}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    topic_name = 'unparsed_recipes'
    recipes = fetch_recipes()
    recipes_json = json.dumps(recipes)
    kafka_producer = connect_kafka_producer()
    publish_message(kafka_producer, topic_name, 'salad-recipe', recipes_json)

    if kafka_producer is not None:
        kafka_producer.close()
