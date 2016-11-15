print('Script starting...')

import pg8000
import json
import redis
import helpers
import time
import pika # something is going wrong with this import on docker-compose
print('Imports finished')


def query_db(query, database='postgres', host='localhost', user='postgres', password='', response=True):
        psql_conn = pg8000.connect(host=host, database=database, user=user, password=password)
        cursor = psql_conn.cursor()
        cursor.execute(query)

        if response:
            result = cursor.fetchall()
            cursor.close()
            psql_conn.close()
            return result
        else:
            cursor.close()
            psql_conn.close()

def callback(ch, method, properties, body):
    print('Message received: %s' % body.decode('utf-8'))
    try:
        result = query_db('SELECT uuid, content FROM survey_response', database='okcollege_dev')

        type_dict, question_table = helpers.construct_type_table('assets/form.json')
        result_table = helpers.process_survey_result(result, type_dict)

        r = redis.StrictRedis(host='localhost')
        r.set('learning:survey_training.csv', result_table.getvalue())
        r.set('learning:survey_features.csv', question_table.getvalue())

    except Exception as e:
        print(e)
        return
    print('Message processed: %s' % body.decode('utf-8'))


if __name__ == '__main__':
    print("Starting...")
    credentials = pika.PlainCredentials('rabbitmq', 'rabbitmq')
    parameters = pika.ConnectionParameters(host='localhost', credentials=credentials)

    print("Attempting connection...")
    while True:
        try:
            mq_conn = pika.BlockingConnection(parameters)
            break
        except Exception as e:
            print("Could not connect to RabbitMQ. Retrying...")
            time.sleep(1)

    channel = mq_conn.channel()
    channel.queue_declare(queue='survey-training-preprocessor')

    print("RabbitMQ connection established.")

    channel.basic_publish(exchange='',
                            routing_key='survey-training-preprocessor',
                            body='set-to-redis')

    channel.basic_consume(callback,
                            queue='survey-training-preprocessor',
                            no_ack=True)
    print("Consuming...")
    channel.start_consuming()
