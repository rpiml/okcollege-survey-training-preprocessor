from preprocessor import query_db
import pg8000
import json
import redis
import helpers
import pika

if __name__ == '__main__':
    with open('assets/example_survey.csv') as example_survey, \
            open('assets/example_features.csv') as example_features, \
            open('assets/example_uuid.txt') as example_uuid, \
            open('assets/example_response.json') as example_response:

        query_db('CREATE TABLE (survey_response) (uuid text, content text)', database='postgres', response=False)

        ins_query = "INSERT INTO survey_response (uuid, content) VALUES ('%s', '%s')" % \
                        ("a", "b")
                    # (example_uuid.read().strip(), example_response.read().strip())
        query_db(ins_query, database='postgres', response=False)

        result = query_db('SELECT uuid, content FROM survey_response', database='postgres')

        type_dict, question_table = helpers.construct_type_table('assets/form.json')
        result_table = helpers.process_survey_result(result, type_dict)


        r = redis.StrictRedis(host='localhost')
        r.set('learning:survey_training.csv', result_table.getvalue())
        r.set('learning:survey_features.csv', question_table.getvalue())

        output_survey = r.get('learning:survey_training.csv')
        output_features = r.get('learning:survey_features.csv')

        assert(example_survey.read().strip() == output_survey.decode('utf-8').strip())
        assert(example_features.read().strip() == output_features.decode('utf-8').strip())
