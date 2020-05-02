import json
import logging
import random
import time

from kafka import KafkaProducer

TOPIC = "ipsparktest"
SLEEP_DURATION = 1
_logger = None


def init_logger():
    global _logger
    _logger = logging.getLogger(__name__)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    _logger.addHandler(ch)
    _logger.setLevel(logging.INFO)


def get_producer():
    kafka_producer = KafkaProducer(bootstrap_servers=["localhost:9092"], )
    return kafka_producer


def get_data():
    s_nouns = ["A dude", "My mom", "The king", "Some guy", "A cat with rabies", "A sloth", "Your homie",
               "This cool guy my gardener met yesterday", "Superman"]
    p_nouns = ["These dudes", "Both of my moms", "All the kings of the world", "Some guys", "All of a cattery's cats",
               "The multitude of sloths living under your bed", "Your homies", "Like, these, like, all these people",
               "Supermen"]
    s_verbs = ["eats", "kicks", "gives", "treats", "meets with", "creates", "hacks", "configures", "spies on",
               "retards", "meows on", "flees from", "tries to automate", "explodes"]
    p_verbs = ["eat", "kick", "give", "treat", "meet with", "create", "hack", "configure", "spy on", "retard",
               "meow on", "flee from", "try to automate", "explode"]
    infinitives = ["to make a pie.", "for no apparent reason.", "because the sky is green.", "for a disease.",
                   "to be able to make toast explode.", "to know more about archeology."]
    sentence = random.choice(s_nouns), random.choice(s_verbs), random.choice(s_nouns).lower() or random.choice(
        p_nouns).lower(), random.choice(infinitives)
    return " ".join(sentence)


def main():
    init_logger()
    _logger.info("starting sending to kafka")
    producer = get_producer()
    for i in range(100):
        data = get_data()
        time.sleep(1)

        _logger.info(f"{str(i)}: {data}")
        producer.send(TOPIC, json.dumps(data).encode('utf-8'))

    _logger.info("stopped sending to kafka")


if __name__ == '__main__':
    main()
