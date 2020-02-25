import json
import random
from datetime import datetime
from time import sleep
from kafka import KafkaProducer


def publish_message(producer_instance, topic_name, key, value):
    try:
        key_bytes = bytes(key, encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))


def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['a49784be7f36511e9a6b60a341003dc2-1378330561.us-east-1.elb.amazonaws.com:9092', 'a4996369ef36511e9a6b60a341003dc2-1583999828.us-east-1.elb.amazonaws.com:9092'], api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


def get_transaction(order_parameters, id):
    merchant_id = random.randint(0, 3)
    item_id = random.randint(0, 3)
    customer_id = random.randint(0, 5)

    transaction = dict()
    transaction['id'] = id
    transaction['datetime'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    transaction['merchant'] = order_parameters['merchant'][merchant_id].copy()
    transaction['item'] = transaction['merchant'].pop('item')[item_id]
    transaction['customer'] = order_parameters['customer'][customer_id]

    return transaction


if __name__ == '__main__':
    with open('./docs/restaurant_data.json') as json_file:
        data = json.load(json_file)

    producer = connect_kafka_producer()

    message_id = 1
    while True:
        message = get_transaction(data, message_id)
        json_message = json.dumps(message, ensure_ascii=False)
        sleep_time = random.random()*10
        sleep(sleep_time)
        print(json_message)
        publish_message(producer, 'orders', 'key', json_message)
        message_id += 1
