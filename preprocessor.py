import pg8000
import json
import helpers

if __name__ == '__main__':
    conn = pg8000.connect(database='okcollege_dev', user='postgres', password='')
    cursor = conn.cursor()
    cursor.execute("SELECT content FROM survey_response")
    result = cursor.fetchall()
    cursor.close()
    conn.close()

    helpers.process_result(result)
