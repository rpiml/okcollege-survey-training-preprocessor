import pg8000
import json
import redis
import helpers

if __name__ == '__main__':
    conn = pg8000.connect(database='okcollege_dev', user='postgres', password='')
    cursor = conn.cursor()
    cursor.execute('SELECT uuid, content FROM survey_response')
    result = cursor.fetchall()
    cursor.close()
    conn.close()

    type_dict, question_table = helpers.construct_type_table('assets/form.json')
    result_table = helpers.process_survey_result(result, type_dict)

    r = redis.StrictRedis(host='localhost')
    r.set('learning:survey_training.csv', result_table.getvalue())
    r.set('learning:survey_features.csv', question_table.getvalue())
