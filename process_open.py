# process_open.py

# pylint: disable-msg=e1101

import settings
from settings import MSSQL_CONNECTION, LEVENSHTEIN_CUTOFF, \
    BIGRAMMS, REGEX_CRITERIA, NEURONET_PICKLE
from load_data import write_records, chunker

from string import punctuation
import pickle
import pandas as pd
import re
from collections import OrderedDict
from sqlalchemy import create_engine
import Levenshtein as lv
from CharVectorizer import CharVectorizer
from timeit import timeit

mssql_engine = create_engine(MSSQL_CONNECTION)
mssql_conn = mssql_engine.connect().connection
mssql_cursor = mssql_conn.cursor()

# removing &-+ from the list of stop characters
# because they can be a valid part of brand name
punctuation = ''.join(c for c in punctuation if c not in '&-+')

original_library = {}
clean_library = {}

def clean_verbatim(verbatim):
    return ''.join(c for c in verbatim.lower().strip() if c not in punctuation)

def clean_for_neuronet(verbatim):
    return clean_verbatim(verbatim)[:100]

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
        clean_answer = func(answer)
        if clean_answer in library:
            record = f'''({serial}, '{variable}', {position}, '{answer}', '{func_name}', {library[clean_answer]}, 1, '{clean_answer}')'''
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
            record = f'''({serial}, '{variable}', {position}, '{answer}', 'lev', {code}, {max_score}, '{candidate}')'''
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
            record = f'''({serial}, '{variable}', {position}, '{answer}', 'svss', 0, 1, '{clean_answer}')'''
            records.append(record)
        if len(clean_answer) < 2:
            record = f'''({serial}, '{variable}', {position}, '{answer}', 'svss_len', 0, 1, '')'''
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
            record = f'''({serial}, '{variable}', {position}, '{answer}', 'bigramms', 0, 1, '{','.join(found_bi)}')'''
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
                    if parsed[i] == parsed[i+1] == parsed[i+2]:
                        is_found = True
                        found_pattern = parsed[i]
                        break
                else:
                    continue
                break
        if is_found:
            record = f'''({serial}, '{variable}', {position}, '{answer}', 'repeats', 0, 1, '{found_pattern}')'''
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
            record = f'''({serial}, '{variable}', {position}, '{answer}', 'num', 0, 1, '{candidate}')'''
            records.append(record) 

    write_records(records, 'open_coded')
    print(f'{len(records)} records')


def ml_neuronet():
    print('starting ml...', end=' ')
    with open('neuronet_2018-06-21.pkl', 'rb') as f:
        mlp_nn = pickle.load(f)

    PUNCTUATIONS = list(punctuation)

    df = pd.read_sql(
        f'select serial, variable, position, answer from open_brands_uncoded where {settings.SERIAL_CRITERIA}', mssql_engine)
    verbatims = df.to_dict('list')
    verbatims['func'] = []
    verbatims['code'] = []
    verbatims['score'] = []
    verbatims['candidate'] = []

    # Bereinigen der neuen Verbatims
    verbatims_clean = []
    for i in range(len(verbatims['answer'])):
        raw = verbatims['answer'][i].lower()
        for p in PUNCTUATIONS:
            raw = raw.replace(p, "")
        if len(raw) > 100:
            raw = raw[:100]
        verbatims_clean.append(raw)

    temp_input = pd.Series(data=verbatims_clean, name='Input')

    # 2. Daten in Format für AI-Algorithmus bringen
    vectorizer = CharVectorizer(
        "abcdefghijklmnopqrstuvwxyzßäöü1234567890", fill_left_char=">", fill_right_char="<")
    # Diese Länge ist fix, da damit der AI-Algorithmus trainiert wurde
    target_length_in = 100

    matrix_in = vectorizer.transform(verbatims_clean, target_length_in)

    chars = "abcdefghijklmnopqrstuvwxyzßäöü1234567890?><"
    columns_in = []
    for j in range(target_length_in):
        for i in chars:
            a = 'in_'+str(i)+str(j+1)
            columns_in.append(a)

    data_in = pd.DataFrame(data=matrix_in, columns=columns_in)

    X = data_in

    # 3. Schätzung
    pred_list = []

    probability = mlp_nn.predict_proba(X)
    proba = pd.DataFrame(data=probability)
    df_p = proba.max(axis=1)

    prediction = mlp_nn.predict(X)
    pred = pd.Series(data=prediction)

    df_pred = pd.concat([temp_input, df_p, pred], axis=1)

    pred_list = df_pred.values.tolist()

    for i in range(len(verbatims['answer'])):
        verbatims['func'].append('ml')
        verbatims['score'].append(pred_list[i][1])
        verbatims['code'].append(pred_list[i][2])
        verbatims['candidate'].append(verbatims_clean[i])

    df_autocoding = pd.DataFrame.from_dict(OrderedDict(verbatims))
    df_coded = df_autocoding[(df_autocoding['score'] >= 0.97) & (
        df_autocoding['answer'].str.contains(r'\\n') == False)]

    df_coded.to_sql('open_coded', mssql_engine, index=False, if_exists='append')

    print('OK')

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
    ml_neuronet()
    print('Processing opens complete\n\n')

if __name__ == '__main__':

    import initialize
    initialize.main()
    print(timeit(main, number=1))
