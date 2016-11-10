import csv
import json
import io

def process_survey_result(result, type_dict):
    response_table = io.StringIO()
    writer = csv.writer(response_table, delimiter=',')
    try:
        for row in result:
            response_vector = []
            for page in row[1]['survey']['pages']:
                for question in page['questions']:
                    if 'answer' not in question:
                        response_vector.append((question['id'], None))
                        continue

                    if question['type'] == 'slider':#['slider', 'choice', 'multi-choice-dropdown']:
                        assert(question['id'] in type_dict)
                        response_vector.append((question['id'], float(question['answer'])))

                    elif question['type'] == 'choice' or question['type'] == 'multi-choice-dropdown':
                        assert(question['id'] in type_dict)
                        i = question['answers'].index(question['answer'])
                        response_vector.append((question['id'], i))

                    elif question['type'] == 'multi-choice':
                        answer = set(question['answer'])
                        for q in question['answers']:
                            name = question['id'] + ':' + q
                            assert(name in type_dict)
                            if q in answer:
                                response_vector.append((name, 1.))
                            else:
                                response_vector.append((name, 0.))
                    else:
                        raise Exception('Malformed question type value in form')

            writer.writerow([str(row[0])] + [v[1] for v in sorted(response_vector, key=lambda x: x[0])])

        return response_table

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
                    if question['type'] == 'slider':
                        type_dict[question['id']] = ('numerical', None)
                    elif question['type'] == 'choice' or question['type'] == 'multi-choice-dropdown':
                        type_dict[question['id']] = ('categorical', str(len(question['answers'])))
                    elif question['type'] == 'multi-choice':
                        for q in question['answers']:
                            type_dict[question['id'] + ':' + q] = ('categorical', 2)
                    else:
                        raise Exception('Malformed question type value in form')

            for k in sorted(type_dict.keys()):
                feature_type, num = type_dict[k]
                writer.writerow([k, feature_type, num])

            return type_dict, question_table

        except LookupError:
            raise Exception('Malformed JSON in form')
