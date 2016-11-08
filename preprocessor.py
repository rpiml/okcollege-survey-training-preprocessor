import pg8000
import json
import helpers

if __name__ == '__main__':
    conn = pg8000.connect(database='okcollege_dev', user='postgres', password='')
    cursor = conn.cursor()
    cursor.execute('SELECT content FROM survey_response')
    result = cursor.fetchall()
    cursor.close()
    conn.close()

    type_dict, question_table = helpers.construct_type_table('../assets/form.json')
    helpers.process_survey_result(result)
