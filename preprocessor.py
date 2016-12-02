import pg8000
import json
import redis
import helpers
import time
import amqpy
import os

class Consumer(amqpy.AbstractConsumer):
    '''
    Class that is instantiated to consume amqp messages
    '''
    def run(self, msg: amqpy.Message):
        print('Message received: %s' % msg.body)
        try:
            result = helpers.query_db('SELECT uuid, content FROM survey_response', database='okcollege_dev')

            type_dict, question_table = helpers.construct_type_table('assets/form.json')
            result_table = helpers.process_survey_result(result, type_dict)

            r = redis.StrictRedis(host=(os.getenv('REDIS_HOST') or 'localhost'))
            r.set('learning:survey_training.csv', result_table.getvalue())
            r.set('learning:survey_features.csv', question_table.getvalue())

        except Exception as e:
            print(e)
            return
        print('Message processed: %s' % msg.body)
        msg.ack()

if __name__ == '__main__':
    print("Starting...")

    print("Attempting connection...")
    while True:
        try:
            conn = amqpy.Connection(userid='rabbitmq', password='rabbitmq', host=(os.getenv('RABBITMQ_HOST') or 'localhost'))
            break
        except Exception as e:
            print("Could not connect to RabbitMQ. Retrying...")
            time.sleep(1)

    channel = conn.channel()
    channel.exchange_declare('preprocessor', 'direct')
    channel.queue_declare('survey-training-preprocessor')
    channel.queue_bind('survey-training-preprocessor', exchange='preprocessor', routing_key='survey-training-preprocessor')

    print("RabbitMQ connection established.")

    channel.basic_publish(amqpy.Message('set-to-redis'), exchange='preprocessor', routing_key='survey-training-preprocessor')

    consumer = Consumer(channel, 'survey-training-preprocessor')
    consumer.declare()

    print("Consuming...")
    while True:
        conn.drain_events(timeout=None)
