import logging

import json

from kafka import KafkaConsumer

from send_email import send_email

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parsed_topic_name = "parsed_recipes"

    consumer = KafkaConsumer(
        parsed_topic_name,
        auto_offset_reset="earliest",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=1000,
    )
    calorie_threshold = 254
    for message in consumer:
        data = message.value
        calories = data["calories"].split(" ")
        if int(calories[0]) > calorie_threshold:
            logging.info("Calorie alert sent to user!!")
            send_email()

    if consumer is not None:
        consumer.close()
