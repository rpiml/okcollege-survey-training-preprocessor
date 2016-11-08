import pg8000
import json

if __name__ == '__main__':
    conn = pg8000.connect(database='okcollege_dev', user='postgres', password='')
    cursor = conn.cursor()
    cursor.execute("SELECT content FROM survey_response")
    result = cursor.fetchall()

    cursor.close()
    conn.close()

    # print(result)

    for row in result:
        # print(row)
        for page in row:
            print(page)
            for question in page['questions']:
                continue
        # print(result[0])
        # jsonstuff = json.dumps(result)
        # continue
        # content_json = json.loads(str(result))
        # print(content_json)


    # print(results)
    # for row in results:
    #     line = ''
    #     for col in row:
    #         line += str(col) + ' | '
    #     print(line)
