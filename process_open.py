# process_open.py

# pylint: disable-msg=e1101

import settings
from settings import MSSQL_CONNECTION, LEVENSHTEIN_CUTOFF, \
    BIGRAMMS, REGEX_CRITERIA, NEURONET_PATH, NEURONET_CUTOFF
from load_data import write_records, chunker

from string import punctuation, whitespace
import pickle
import pandas as pd
import re
from collections import OrderedDict, defaultdict, deque
from sqlalchemy import create_engine
import Levenshtein as lv
from CharVectorizer import CharVectorizer
import itertools

mssql_engine = create_engine(MSSQL_CONNECTION)
mssql_conn = mssql_engine.connect().connection
mssql_cursor = mssql_conn.cursor()

# removing &-+ from the list of stop characters
# because they can be a valid part of brand name
punctuation = ''.join(c for c in punctuation if c not in '&-+')

original_library = {}
clean_library = {}

def clean_verbatim(verbatim):
    return ''.join(c for c in verbatim.lower() if c not in punctuation)

def load_library():
    print('Loading library...', end=' ')
    global original_library, clean_library
    original_library = {verbatim : code for verbatim, code in mssql_engine.execute('select verbatim, code from open_library').fetchall()}
    clean_library = {clean_verbatim(verbatim): code for verbatim, code in original_library.items() if clean_verbatim(verbatim)}

    print('OK')

def empty_open_tables():

    print('Emptying open tables...', end=' ')
    mssql_engine.execute(f'delete from open_tokenized where {settings.SERIAL_CRITERIA}')
    mssql_engine.execute(f'delete from open_coded where {settings.SERIAL_CRITERIA}')
    print('OK')

def tokenize_answers():
    
    print('Tokenizing answers...', end=' ')
    records = []
    for serial, variable, answer in mssql_engine.execute(f'''
        select o.serial, o.variable, o.answer
        from import_open as o
            join open_variables as v
                on o.variable = v.variable
        where {settings.SERIAL_CRITERIA}
            and o.answer like '%' + char(10) + '%'
            and v.type = 'brand'
        ''').fetchall():
        tokenized_answers = answer.split('\n')
        for index, tokenized_answer in enumerate(tokenized_answers):
            if tokenized_answer:
                record = f'''({serial}, '{variable}', {index}, char(10), '{tokenized_answer}')'''
                records.append(record)
    write_records(records, 'open_tokenized')
    print(f'{len(records)} records')

def lookup(func, func_name, library):

    print(f'Coding using {func_name}...', end=' ')
    records = []
    for serial, variable, position, answer in mssql_engine.execute(f'''
        select serial, variable, position, answer
        from open_brands_uncoded
        where {settings.SERIAL_CRITERIA}
        ''').fetchall():
        clean_answer = func(answer).strip()
        if clean_answer in library:
            record = f'''({serial}, '{variable}', {position}, '{answer.replace("'", r"''")}', '{func_name}', {library[clean_answer]}, 1, '{clean_answer}')'''
            records.append(record)
    write_records(records, 'open_coded')
    print(f'{len(records)} records')

def lev():

    print(f'Coding using levenshtein...', end=' ')
    records = []
    for serial, variable, position, answer in mssql_engine.execute(f'''
        select serial, variable, position, answer
        from open_brands_uncoded
        where {settings.SERIAL_CRITERIA}
        ''').fetchall():
        clean_answer = clean_verbatim(answer)
        max_score, code, candidate = 0.0, 0, ''
        for v, c in clean_library.items():
            score = lv.ratio(clean_answer, v)
            if score > LEVENSHTEIN_CUTOFF and score > max_score:
                max_score, code, candidate = score, c, v
        if max_score > LEVENSHTEIN_CUTOFF:
            record = f'''({serial}, '{variable}', {position}, '{answer.replace("'", r"''")}', 'lev', {code}, {max_score}, '{candidate}')'''
            records.append(record)

    write_records(records, 'open_coded')
    print(f'{len(records)} records')

def svss():
    print(f'Coding using svss...', end=' ')
    delete = ['ich weiss nicht', 'ich weiß nicht', 'ich weis nicht',
               'ich weiß nicht was', 'ich weiß', 'ich hatte bin', 'ich habe bin']
    records = []
    for serial, variable, position, answer in mssql_engine.execute(f'''
        select serial, variable, position, answer
        from open_brands_uncoded
        where {settings.SERIAL_CRITERIA}
        ''').fetchall():
        clean_answer = clean_verbatim(answer)
        if clean_answer in delete:
            record = f'''({serial}, '{variable}', {position}, '{answer.replace("'", r"''")}', 'svss', 0, 1, '{clean_answer}')'''
            records.append(record)
        if len(clean_answer) < 2:
            record = f'''({serial}, '{variable}', {position}, '{answer.replace("'", r"''")}', 'svss_len', 0, 1, '')'''
            records.append(record)

    write_records(records, 'open_coded')
    print(f'{len(records)} records')

def bigramms():

    print(f'Coding using bigramms...', end=' ')
    records = []
    for serial, variable, position, answer in mssql_engine.execute(f'''
        select serial, variable, position, answer
        from open_brands_uncoded
        where {settings.SERIAL_CRITERIA}
        ''').fetchall():
        clean_answer = clean_verbatim(answer)
        count = 0
        found_bi = []
        for bi in BIGRAMMS:
            if bi in clean_answer:
                freq = clean_answer.count(bi) 
                count += freq
                found_bi.append(bi)
        if count >= 3:
            record = f'''({serial}, '{variable}', {position}, '{answer.replace("'", r"''")}', 'bigramms', 0, 1, '{','.join(found_bi)}')'''
            records.append(record)            

    write_records(records, 'open_coded')
    print(f'{len(records)} records')


def repeats():

    print(f'Coding using repeats...', end=' ')

    records = []
    for serial, variable, position, answer in mssql_engine.execute(f'''
        select serial, variable, position, answer
        from open_brands_uncoded
        where {settings.SERIAL_CRITERIA}
        ''').fetchall():
        clean_answer = clean_verbatim(answer)
        is_found = False
        for chunk in range(3, 0, -1):
            parsed = [clean_answer[i:i+chunk] for i in range(0, len(clean_answer), chunk)]
            if len(parsed) > 2:
                for i in range(len(parsed) - 2):
                    if parsed[i] == parsed[i+1] == parsed[i+2] \
                    and parsed[i] not in '   ' and parsed[i] not in '...':
                        is_found = True
                        found_pattern = parsed[i]
                        break
                else:
                    continue
                break
        if is_found:
            record = f'''({serial}, '{variable}', {position}, '{answer.replace("'", r"''")}', 'repeats', 0, 1, '{found_pattern}')'''
            records.append(record)

    write_records(records, 'open_coded')
    print(f'{len(records)} records')


def numbers():

    print(f'Coding using numbers...', end=' ')
    records = []
    for serial, variable, position, answer in mssql_engine.execute(f'''
        select serial, variable, position, answer
        from open_brands_uncoded
        where {settings.SERIAL_CRITERIA}
        ''').fetchall():
        clean_answer = clean_verbatim(answer)
        found = re.findall(REGEX_CRITERIA, clean_answer)

        if found:            
            candidate = ','.join(found)
            record = f'''({serial}, '{variable}', {position}, '{answer.replace("'", r"''")}', 'num', 0, 1, '{candidate}')'''
            records.append(record) 

    write_records(records, 'open_coded')
    print(f'{len(records)} records')


def ml_neuronet():
    print('coding using ml...', end=' ')
    with open(NEURONET_PATH, 'rb') as f:
        mlp_nn = pickle.load(f)
    vectorizer = CharVectorizer(
        "abcdefghijklmnopqrstuvwxyzßäöü1234567890", fill_left_char=">", fill_right_char="<")

    target_length_in = 100

    records = []
    for serial, variable, position, answer in mssql_engine.execute(f'''
        select serial, variable, position, answer
        from open_brands_uncoded
        where {settings.SERIAL_CRITERIA}
        ''').fetchall():
        clean_answer = clean_verbatim(answer)[:100]
        data_in = vectorizer.transform([clean_answer], target_length_in)
        X = pd.DataFrame(data=data_in)
        probability = mlp_nn.predict_proba(X)
        max_prob = probability.max(axis = 1)[0]
        if max_prob >= NEURONET_CUTOFF:            
            prediction = mlp_nn.predict(X)[0]
            record = f'''({serial}, '{variable}', {position}, '{answer.replace("'", r"''")}', 'ml', {prediction}, {max_prob}, '')'''
            records.append(record)
        
    write_records(records, 'open_coded')
    print(f'{len(records)} records')


def main():

    print('PROCESSING OPEN')
    load_library()
    empty_open_tables()
    tokenize_answers()
    lookup(lambda x: x, 'lookup', original_library)
    lookup(clean_verbatim, 'lookup_clean', clean_library)
    lev()
    svss()
    bigramms()
    repeats()
    numbers()
    print('Processing opens complete', end='\n\n')


if __name__ == '__main__':
    
    from timeit import timeit
    import initialize
    initialize.main()
    print(timeit(main, number=1))
