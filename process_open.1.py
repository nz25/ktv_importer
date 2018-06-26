# process_open.py

import settings as s

import string
import pandas as pd
import pickle
import re
from collections import OrderedDict
from sqlalchemy import create_engine
import Levenshtein as lv
from CharVectorizer import CharVectorizer
from timeit import timeit

engine = create_engine(s.mssql_connection)


def tokenize():
    
    print('starting tokenize...', end=' ')
    engine.execute(f'delete from open_tokenized where {s.serial_criteria}')
    df = pd.read_sql(f'select * from import_open where {s.serial_criteria}', engine)
    df_brand = pd.read_sql(
        "select variable from open_variables where type = 'brand'", engine)
    questions = df_brand.values.tolist()
    questions = [q[0] for q in questions]
    df = df[(df['answer'].str.contains('\n') == True)
            & (df['variable'].isin(questions))]
    df = df.reset_index(drop=True)
    df_token = pd.DataFrame(None, columns=[
                            'serial', 'variable', 'position', 'separator', 'answer', 'answer_token'])

    for i in range(len(df)):
        token = df.loc[(i, 'answer')].split('\n')
        token[:] = filter(None, token)
        for j in range(len(token)):
            row = df.iloc[[i]].copy(deep=True)
            row['answer_token'] = token[j]
            row['position'] = j+1
            row['separator'] = '\n'
            df_token = df_token.append(row, ignore_index=True)
    df_token = df_token[['serial', 'variable', 'position',
                         'separator', 'answer', 'answer_token']]
    df_token = df_token[['serial', 'variable',
                         'position', 'separator', 'answer_token']]
    df_token = df_token.rename({'answer_token': 'answer'}, axis=1)

    df_token.to_sql('open_tokenized', engine, index=False, if_exists='append')

    print('OK')

def lev():

    print('starting lev...', end=' ')
    df_verbatims = pd.read_sql(
        'select verbatim, code from open_library', engine)
    bib_verbatims = df_verbatims.values.tolist()

    PUNCTUATIONS = list(string.punctuation)

    bib_clean = []
    for i in range(len(bib_verbatims)):
        raw = str(bib_verbatims[i][0]).lower()
        for p in PUNCTUATIONS:
            raw = raw.replace(p, "")
        bib_clean.append([raw, bib_verbatims[i][1]])

    KTV_BIB = {}
    for i in range(len(bib_clean)):
        key = bib_clean[i][1]
        value = bib_clean[i][0]
        if key in KTV_BIB.keys():
            if value not in KTV_BIB[key]:
                KTV_BIB[key].append(value)
        else:
            KTV_BIB.update({key: [value]})

    #df_coded = pd.DataFrame(data = None, columns = ['serial', 'variable', 'answer', 'func', 'code', 'score', 'candidate'])

    df = pd.read_sql(
        f'select serial, variable, position, answer from open_brands_uncoded where {s.serial_criteria}', engine)
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
        verbatims_clean.append(raw)

    for i in range(len(verbatims['answer'])):
        temp = 0
        for k, v in KTV_BIB.items():
            if verbatims_clean[i] in v and temp == 0:
                verbatims['func'].append('lev')
                verbatims['code'].append(k)
                verbatims['score'].append(1)
                verbatims['candidate'].append(verbatims_clean[i])
                temp = 1
        if temp == 0:
            lev_scores = []
            for j in range(len(bib_clean)):
                pct = lv.ratio(verbatims_clean[i], bib_clean[j][0])
                lev_scores.append(pct)
            win = max(lev_scores)
            if win != 0:
                verbatims['func'].append('lev')
                stelle = lev_scores.index(win)
                verbatims['code'].append(bib_clean[stelle][1])
                verbatims['score'].append(win)
                verbatims['candidate'].append(bib_clean[stelle][0])
            else:
                verbatims['func'] = ''
                verbatims['code'].append('')
                verbatims['score'].append('')
                verbatims['candidate'].append('')
                temp = 1

    df_autocoding = pd.DataFrame.from_dict(OrderedDict(verbatims))
    df_autocoding = df_autocoding[df_autocoding.score >= 0.95]

    df_autocoding.to_sql('open_coded', engine, index=False, if_exists='append')

    print('OK')

def svss():
    
    print('starting svss...', end=' ')
    PUNCTUATIONS = list(string.punctuation)

    df = pd.read_sql(
        f'select serial, variable, position, answer from open_brands_uncoded where {s.serial_criteria}', engine)
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
        verbatims_clean.append(raw)

    delete = ['ich weiss nicht', 'ich weiß nicht', 'ich weis nicht',
              'ich weiß nicht was', 'ich weiß', 'ich hatte bin', 'ich habe bin']

    for i in range(len(verbatims['answer'])):
        if verbatims_clean[i] in delete or len(verbatims_clean[i]) < 2:
            verbatims['func'].append('svss')
            verbatims['code'].append(0)
            verbatims['score'].append(1)
            verbatims['candidate'].append(verbatims_clean[i])
        else:
            verbatims['func'].append('nan')
            verbatims['code'].append('')
            verbatims['score'].append('')
            verbatims['candidate'].append('')

    df_autocoding = pd.DataFrame.from_dict(OrderedDict(verbatims))
    df_coded = df_autocoding[df_autocoding.func != 'nan']

    df_coded.to_sql('open_coded', engine, index=False, if_exists='append')

    print('OK')

def bigramms():
    
    print('starting bigramms...', end=' ')
    df = pd.read_sql(
        f'select serial, variable, position, answer from open_brands_uncoded where {s.serial_criteria}', engine)
    verbatims = df.to_dict('list')
    verbatims['func'] = []
    verbatims['code'] = []
    verbatims['score'] = []
    verbatims['candidate'] = []



    for i in range(len(verbatims['answer'])):
        count = 0
        found_bi = []
        for bi in bigramms:
            if bi in verbatims['answer'][i]:
                freq = verbatims['answer'][i].count(bi)
                count += freq
                found_bi.append(bi)
        if count >= 3:
            candi = ', '.join(found_bi)
            verbatims['func'].append('bigr')
            verbatims['code'].append(0)
            verbatims['score'].append(1)
            verbatims['candidate'].append(candi)

        else:
            verbatims['func'].append('nan')
            verbatims['code'].append('')
            verbatims['score'].append('')
            verbatims['candidate'].append('')

    df_autocoding = pd.DataFrame.from_dict(OrderedDict(verbatims))
    df_coded = df_autocoding[df_autocoding.func != 'nan']

    df_coded.to_sql('open_coded', engine, index=False, if_exists='append')

    print('OK')

def repeats():
    print('starting repeats...', end=' ')
    df = pd.read_sql(
        f'select serial, variable, position, answer from open_brands_uncoded where {s.serial_criteria}', engine)
    verbatims = df.to_dict('list')
    verbatims['func'] = []
    verbatims['code'] = []
    verbatims['score'] = []
    verbatims['candidate'] = []

    n = 2

    for i in range(len(verbatims['answer'])):
        parsed = [verbatims['answer'][i][j:j+n] for j in range(0, len(verbatims['answer'][i]), n)]
        temp = 0
        for j in range(len(parsed)-3):
            if parsed[j] == parsed[j+1] and parsed[j+1] == parsed[j+2] and parsed[j+2] == parsed[j+3]:
                found = parsed[j]
                temp = 1
        if temp == 1:
            verbatims['func'].append('rep')
            verbatims['code'].append(0)
            verbatims['score'].append(1)
            verbatims['candidate'].append(found)
            temp = 1
        else:
            verbatims['func'].append('nan')
            verbatims['code'].append('')
            verbatims['score'].append('')
            verbatims['candidate'].append('')

    for i in range(len(verbatims['answer'])):
        parsed = list(verbatims['answer'][i])
        for j in range(len(parsed)-3):
            if parsed[j] == parsed[j+1] and parsed[j+1] == parsed[j+2] and parsed[j+2] == parsed[j+3]:
                verbatims['func'][i] = 'rep'
                verbatims['code'][i] = 0
                verbatims['score'][i] = 1
                verbatims['candidate'][i] = parsed[j]



    df_autocoding = pd.DataFrame.from_dict(OrderedDict(verbatims))
    df_coded = df_autocoding[df_autocoding.func != 'nan']

    df_coded.to_sql('open_coded', engine, index=False, if_exists='append')

    print('OK')

def numbers():
    
    print('starting numbers...', end=' ')
    df = pd.read_sql(
        f'select serial, variable, position, answer from open_brands_uncoded where {s.serial_criteria}', engine)
    verbatims = df.to_dict('list')
    verbatims['func'] = []
    verbatims['code'] = []
    verbatims['score'] = []
    verbatims['candidate'] = []

    for i in range(len(verbatims['answer'])):
        if bool(re.findall(r'[\d]+[a-zA-ZöüäÖÜÄ]+[\d]+', verbatims['answer'][i])):
            found = re.findall(r'[\d]+[a-zA-ZöüäÖÜÄ]+[\d]', verbatims['answer'][i])
            candi = ', '.join(found)
            verbatims['func'].append('num')
            verbatims['code'].append(0)
            verbatims['score'].append(1)
            verbatims['candidate'].append(candi)

        else:
            verbatims['func'].append('nan')
            verbatims['code'].append('')
            verbatims['score'].append('')
            verbatims['candidate'].append('')

    df_autocoding = pd.DataFrame.from_dict(OrderedDict(verbatims))
    df_coded = df_autocoding[df_autocoding.func != 'nan']

    df_coded.to_sql('open_coded', engine, index=False, if_exists='append')

    print('OK')
    

def ml_neuronet():
    print('starting ml...', end=' ')
    with open('neuronet_2018-06-21.pkl', 'rb') as f:
        mlp_nn = pickle.load(f)

    PUNCTUATIONS = list(string.punctuation)

    df = pd.read_sql(
        f'select serial, variable, position, answer from open_brands_uncoded where {s.serial_criteria}', engine)
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

    df_coded.to_sql('open_coded', engine, index=False, if_exists='append')

    print('OK')

def main():
    
    engine.execute(f'delete from open_coded where {s.serial_criteria}')
    tokenize()
    lev()
    svss()
    bigramms()
    repeats()
    numbers()
    ml_neuronet()


if __name__ == '__main__':
    import load_data
    load_data.read_increment()
    print(timeit(main, number=1))
