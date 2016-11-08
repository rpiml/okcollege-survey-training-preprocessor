def _compute_value(question):
    if question['type'] == 'slider':
        return question['answer']
    elif question['type'] == 'multi-choice':
        answers = dict(zip(question['answers'], range(len(question['answers']))))
        answer = question['answer']

        value = 0

        for ans in answer:
            i = answers[ans]
            value &= 2 ** i

        return value

    elif question['type'] == 'choice':
        pass
    elif question['type'] == 'multi-choice-dropdown':
        pass
    else:
        raise Exception("Malformed question type value")

def process_result(result):
    try:
        for row in result:
            for page in row[0]['survey']['pages']:
                for question in page['questions']:
                    print(type(question))
                    value = _compute_value(question)

    except LookupError:
        raise Exception("Malformed JSON object")

def construct_type_table(form):
    pass
