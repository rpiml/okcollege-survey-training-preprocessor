import pg8000
import json
import redis
import helpers
import unittest

class TestHelperFunctions(unittest.TestCase):
    def setUp(self):
        self.example_survey = open('test_assets/example_survey.csv')
        self.example_features = open('test_assets/example_features.csv')
        self.example_uuid = open('test_assets/example_uuid.txt')
        self.example_response = open('test_assets/example_response.json')

        helpers.query_db('CREATE TABLE survey_response (uuid uuid, content json)', database='postgres', response=False, autocommit=True)

        ins_query = "INSERT INTO survey_response VALUES ('%s', '%s')" % \
                    ("782786f0-a6fa-11e6-8a53-795ffb01e23b", json.dumps(json.load(self.example_response)))

        helpers.query_db(ins_query, database='postgres', response=False, autocommit=True)

        result = helpers.query_db('SELECT uuid, content FROM survey_response', database='postgres')

        type_dict, question_table = helpers.construct_type_table('test_assets/form.json')
        result_table = helpers.process_survey_result(result, type_dict)

        self.r = redis.StrictRedis(host='localhost')
        self.r.set('learning:survey_training.csv', result_table.getvalue())
        self.r.set('learning:survey_features.csv', question_table.getvalue())

    def tearDown(self):
        self.example_survey.close()
        self.example_features.close()
        self.example_uuid.close()
        self.example_response.close()

        helpers.query_db('DROP TABLE survey_response', database='postgres', response=False, autocommit=True)

    def test_survey_csv(self):
        output_survey = self.r.get('learning:survey_training.csv')
        output_survey_text = output_survey.decode('utf-8').rstrip()
        example_survey_text = self.example_survey.read().rstrip()

        self.assertEqual(output_survey_text, example_survey_text)

    def test_features_csv(self):
        output_features = self.r.get('learning:survey_features.csv')
        output_features_list = output_features.decode('utf-8').split('\r\n')
        example_features_list = self.example_features.read().split('\n')

        self.assertEqual(len(example_features_list), len(output_features_list))

        for i, line in enumerate(example_features_list):
            self.assertEqual(line.strip(), output_features_list[i].strip())


if __name__ == '__main__':
    unittest.main()
