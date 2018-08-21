import pika
import logging
import json
import os

logging.getLogger('pika').setLevel(logging.INFO)
logger = logging.getLogger(__name__)

def get_amqp_connection_channel(message):
    AMQPUSERNAME = os.getenv('AMQPUSERNAME', "not_set")
    AMQPPASSWORD = os.getenv('AMQPPASSWORD', "not_set")
    AMQPHOST = os.getenv('AMQPHOST', "not_set")

    credentials = pika.PlainCredentials(AMQPUSERNAME, AMQPPASSWORD)
    ssl_options = {
        "ca_certs": "../../ssl/ca.cert",
        "certfile": "../../ssl/rabbitmq.cert",
        "keyfile": "../../ssl/rabbitmq.key"
    }
    params = pika.ConnectionParameters(
        host=AMQPHOST,
        port=5671,
        credentials=credentials,
        connection_attempts=3,
        retry_delay=2,
        ssl=True,
        ssl_options=ssl_options,
        socket_timeout=100,
        heartbeat_interval=3
        )
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    d = channel.basic_publish(exchange='cron_exchange',
                  routing_key='CRONAMQP',
                 body=json.dumps(message))
    print ('message sent status =', d)
    #connection.close()
    return d

