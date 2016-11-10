import pg8000
import json
import redis
import helpers
import pika

def callback(ch, method, properties, body):
    print('Message received: %s' % body.decode('utf-8'))
    try:
        psql_conn = pg8000.connect(database='okcollege_dev', user='postgres', password='')
        cursor = psql_conn.cursor()
        cursor.execute('SELECT uuid, content FROM survey_response')
        result = cursor.fetchall()
        cursor.close()
        psql_conn.close()

        type_dict, question_table = helpers.construct_type_table('../assets/form.json')
        result_table = helpers.process_survey_result(result, type_dict)

        r = redis.StrictRedis(host='localhost')
        r.set('learning:survey_training.csv', result_table.getvalue())
        r.set('learning:survey_features.csv', question_table.getvalue())
    except Exception as e:
        print(type(e))
        print(e.args)
        print(e)
        return
    print('Message processed: %s' % body.decode('utf-8'))


if __name__ == '__main__':
    credentials = pika.PlainCredentials('rabbitmq', 'rabbitmq')
    parameters = pika.ConnectionParameters(host='localhost', credentials=credentials)
    mq_conn = pika.BlockingConnection(parameters)
    channel = mq_conn.channel()
    channel.queue_declare(queue='survey-training-preprocessor')

    channel.basic_publish(exchange='',
                            routing_key='survey-training-preprocessor',
                            body='set-to-redis')

    channel.basic_consume(callback,
                            queue='survey-training-preprocessor',
                            no_ack=True)
    channel.start_consuming()
