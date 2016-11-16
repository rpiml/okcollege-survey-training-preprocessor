import pg8000
import json
import redis
import helpers

if __name__ == '__main__':
    with open('test_assets/example_survey.csv') as example_survey, \
            open('test_assets/example_features.csv') as example_features, \
            open('test_assets/example_uuid.txt') as example_uuid, \
            open('test_assets/example_response.json') as example_response:

        helpers.query_db('CREATE TABLE survey_response (uuid uuid, content json)', database='postgres', response=False, autocommit=True)

        ins_query = "INSERT INTO survey_response VALUES ('%s', '%s')" % \
                    ("782786f0-a6fa-11e6-8a53-795ffb01e23b", json.dumps(json.load(example_response)))

        helpers.query_db(ins_query, database='postgres', response=False, autocommit=True)

        result = helpers.query_db('SELECT uuid, content FROM survey_response', database='postgres')

        type_dict, question_table = helpers.construct_type_table('assets/form.json')
        result_table = helpers.process_survey_result(result, type_dict)

        r = redis.StrictRedis(host='localhost')
        r.set('learning:survey_training.csv', result_table.getvalue())
        r.set('learning:survey_features.csv', question_table.getvalue())

        output_survey = r.get('learning:survey_training.csv')
        output_features = r.get('learning:survey_features.csv')

        example_survey_text = example_survey.read().rstrip()
        output_survey_text = output_survey.decode('utf-8').rstrip()

        print('Following 2 lines should be identical:')
        print(example_survey_text)
        print('-' * 30)
        print(output_survey_text)
        assert(example_survey_text == output_survey_text)

        example_features_list = example_features.read().split('\n')
        output_features_list = output_features.decode('utf-8').split('\r\n')

        print('%d and %d should be identical' % (len(example_features_list), len(output_features_list)))
        print(len(example_features_list), len(output_features_list))
        assert(len(example_features_list) == len(output_features_list))

        print('\nFollowing 2 tables should be identical:')
        for i, line in enumerate(example_features_list):
            print('%s <-> %s' % (line.strip(), output_features_list[i].strip()))
            assert(line.strip() == output_features_list[i].strip())
