import json
import logging

from kafka import KafkaConsumer, KafkaProducer


def connect_kafka_producer():
    try:
        producer = KafkaProducer(bootstrap_servers="localhost:9092")
        logging.info("Connected to kafka-producer successfully..")

        return producer
    except Exception as exception:
        raise Exception("Connection failed", exception)


def publish_message(
    producer_instance: KafkaProducer,
    topic_name: str,
    message_key: str,
    message_value: json,
):
    """
    Publish the message into topics based on the message key

    :param KafkaProducer producer_instance: KafkaProducer object
    :param str topic_name: Topic where message will be published
    :param str message_key: Key to associate with the message
    :param Json message_value: Actual message
    """
    try:
        message_key_bytes = bytes(message_key, encoding="utf-8")
        message_value_bytes = message_value.encode("utf-8")
        producer_instance.send(
            topic_name, key=message_key_bytes, value=message_value_bytes
        )

        # block until all async messages are sent
        producer_instance.flush()

        logging.info(f"Message published to {topic_name} !!")
    except Exception as exception:
        logging.error(f"Couldn't publish message to {topic_name}: {exception}")


def retrieve_message(topic_name: str):
    """
    Connects to kafka consumer and retrieves the details

    :param str topic_name: Topic name

    :return KafkaConsumer: KafkaConsumer object
    """
    try:
        consumer = KafkaConsumer(
            topic_name,
            auto_offset_reset="earliest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=1000,
        )

        return consumer
    except Exception as exception:
        raise Exception(f"Could not connect to consumer: {exception}")


if __name__ == "__main__":
    """
    Reads from `unparsed_recipes` topic, parse the data and
    then publish the parsed data into `parsed_recipe` topic
    """
    logging.basicConfig(level=logging.INFO)

    logging.info("Running Consumer...")
    consumer = retrieve_message("unparsed_recipes")

    parsed_records = []
    producer = connect_kafka_producer()

    for message in consumer:
        record = message.value
        data = {
            "title": record["title"],
            "description": record["description"],
            "author": record["author"],
            "calories": record["nutrients"]["calories"],
        }
        logging.info("Publishing parsed data into parsed_recipes topic in producer..")
        publish_message(
            producer, "parsed_recipes", "parsed_salad_recipe", json.dumps(data)
        )

    logging.info("Closing Consumer::::")
    if consumer is not None:
        consumer.close()
