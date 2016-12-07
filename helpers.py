import csv
import json
import pg8000
import io
import os

'''
Collection of helper functions to use in the survey training preprocessor
'''

def query_db(query, database='postgres', host=None, user='postgres', password='', response=True, autocommit=False):
    '''
    Function that connects to the specified PostgreSQL database and executes the given query
    '''
    if host is None:
        host = os.getenv("PG_HOST") or "localhost"

    psql_conn = pg8000.connect(host=host, database=database, user=user, password=password)
    psql_conn.autocommit = autocommit

    print('Querying DB')

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

def process_survey_result(result, type_dict):
    '''
    This function takes:
        - the result of a database query (as a list of JSON objects)
        - a dictionary of format {question_id : ('numerical' || 'categorical', categorical_count)}

    and returns an in-memory csv file where:
        - rows are individual survey responses
        - columns are features
        - the first column contains the survey response uuids
    '''
    response_table = io.StringIO()
    writer = csv.writer(response_table, delimiter='\t')
    for row in result:
        try:
            response_vector = []
            seen_questions = set()
            for page in row[1]['survey']['pages']:
                for question in page['questions']:
                    if question['id'] not in type_dict:
                        continue

                    seen_questions.add((question['id']))

                    if 'answer' not in question:
                        response_vector.append((question['id'], None))
                        continue

                    if question['type'] == 'slider':
                        response_vector.append((question['id'], question['answer']))

                    elif question['type'] == 'choice' or question['type'] == 'multi-choice-dropdown':
                        i = question['answers'].index(question['answer'])
                        response_vector.append((question['id'], i))

                    elif question['type'] == 'multi-choice':
                        answer = set(question['answer'])
                        for q in question['answers']:
                            name = question['id'] + ':' + q
                            if q in answer:
                                response_vector.append((name, 1.))
                            else:
                                response_vector.append((name, 0.))
                    else:
                        response_vector.append((question['id'], None))
            unseen_questions = set(type_dict.keys()) - seen_questions
            for q in unseen_questions:
                response_vector.append((q, None))
            writer.writerow([str(row[0])] + [v[1] for v in sorted(response_vector, key=lambda x: x[0])])

        except LookupError:
            print('Malformed JSON object in %s', str(row[0]))

    return response_table


def construct_type_table(form_loc='assets/form.json'):
    '''
    This function takes:
        - a json filepath containing the form template

    and returns:
        - a dictionary of format {question_id : ('numerical' || 'categorical', categorical_count)}
        - a csv file where rows are of the format:
            question_id     question_type       categorical_count (if any)

        Note:
            Multi-choice questions are broken up into a series of categorical
            questions of count 2 (i.e. booleans)
    '''
    with open(form_loc) as f:
        form = json.load(f)
        type_dict = {}
        question_table = io.StringIO()
        writer = csv.writer(question_table, delimiter='\t')
        try:
            for page in form['pages']:
                for question in page['questions']:
                    if question['type'] == 'slider':
                        type_dict[question['id']] = ('numerical', None)
                    elif question['type'] == 'choice' or question['type'] == 'multi-choice-dropdown':
                        type_dict[question['id']] = ('categorical', str(len(question['answers'])))
                    elif question['type'] == 'multi-choice':
                        for q in question['answers']:
                            type_dict[question['id'] + ':' + q] = ('categorical', 2)
                    elif question['type'] == 'text':
                        type_dict[question['id']] =  ('text', 1)
                    else:
                        raise Exception('Unknown question type {} in form'.format(question['type']))

            for k in sorted(type_dict.keys()):
                feature_type, num = type_dict[k]
                writer.writerow([k, feature_type, num])

            return type_dict, question_table

        except LookupError:
            raise Exception('Malformed JSON in form')
