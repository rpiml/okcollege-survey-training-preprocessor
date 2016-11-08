import csv
import json
import io

def _compute_values(question, type_dict):
    if question['type'] in ['slider', 'choice', 'multi-choice-dropdown']:
        type_dict[question['id']] = question['type']
        writer.writerow([question['id'], question['type']])
    elif question['type'] == 'multi-choice':
        for q in question['answers']:
            type_dict[question['id'] + ':' + q] = 'boolean'
            writer.writerow([question['id'] + ':' + q, 'boolean'])
    else:
        raise Exception("Malformed question type value in survey_response")
        # if question['type'] == 'slider':
        #     return [question['answer']]
        # elif question['type'] == 'multi-choice':
        #     answers = dict(zip(question['answers'], range(len(question['answers']))))
        #     answer = question['answer']
        #
        #     value = 0
        #
        #     for ans in answer:
        #         i = answers[ans]
        #         value &= 2 ** i
        #
        #     return value
        #
        # elif question['type'] == 'choice':
        #     pass
        # elif question['type'] == 'multi-choice-dropdown':
        #     pass

def process_survey_result(result, type_dict):
    response_table = io.StringIO()
    writer = csv.writer(response_table, delimiter=',')
    try:
        for row in result:
            response_vector = []
            for page in row[0]['survey']['pages']:
                for question in page['questions']:
                    if question['type'] == 'slider':#['slider', 'choice', 'multi-choice-dropdown']:

                        response_vector.append(float(question['answer']))
                        # response_vector.append(float())
                        # type_dict[question['id']] = question['type']
                        # writer.writerow([question['id'], question['type']])

                    elif question['type'] == 'choice' or question['type'] == 'multi-choice-dropdown':
                        pass
                    elif question['type'] == 'multi-choice':
                        for q in question['answers']:
                            name = question['id'] + ':' + q
                            # type_dict[question['id'] + ':' + q] = 'boolean'
                            # writer.writerow([question['id'] + ':' + q, 'boolean'])
                    else:
                        raise Exception('Malformed question type value in form')
                    # print(type(question))
                    # values = _compute_values(question, type_dict)
            writer.writerow([v for v in sorted(response_vector, key=lambda x: x[0])])

    except LookupError:
        raise Exception('Malformed JSON object in survey_response')

def construct_type_table(form_loc='assets/form.json'):
    with open(form_loc) as f:
        form = json.load(f)
        type_dict = {}
        question_table = io.StringIO()
        writer = csv.writer(question_table, delimiter=',')
        try:
            for page in form['pages']:
                for question in page['questions']:
                    # print(type(question))
                    if question['type'] == 'slider':
                        type_dict[question['id']] = 'numerical'
                        writer.writerow([question['id'], 'numerical'])
                    elif question['type'] == 'choice' or question['type'] == 'multi-choice-dropdown':
                        type_dict[question['id']] = ('categorical', len(question['answers']))
                        writer.writerow([question['id'] + ':' + q, 'categorical', len(question['answers'])])
                    elif question['type'] == 'multi-choice':
                        for q in question['answers']:
                            type_dict[question['id'] + ':' + q] = ('categorical', 2)
                            writer.writerow([question['id'] + ':' + q, 'categorical', str(2)])
                    else:
                        raise Exception('Malformed question type value in form')
                        # answer = set(question['answer'])
                        # for q in question['answers']:
                        #     if q in answer:
                        #         questions[question['id'] + ':' + q] = 1.
                        #         writer.writerow()
                        #     else:
                        #         questions[question['id'] + ':' + q] = 0.




            return type_dict, question_table
        except LookupError:
            raise Exception('Malformed JSON in form')
