# process_open.py

# pylint: disable-msg=e1101

import settings
from settings import mssql_connection, LEVENSHTEIN_CUTOFF

from string import punctuation
import pickle
import pandas as pd
import re
from collections import OrderedDict
from sqlalchemy import create_engine
import Levenshtein as lv
from CharVectorizer import CharVectorizer
from timeit import timeit

mssql_engine = create_engine(mssql_connection)
mssql_conn = mssql_engine.connect().connection
mssql_cursor = mssql_conn.cursor()

# removing &-+ from the list of stop characters
# because they can be a valid part of brand name
punctuation = ''.join(c for c in punctuation if c not in '&-+')

original_library = {}
clean_library = {}

def clean_verbatim(verbatim):
    return ''.join(c for c in verbatim.lower().strip() if c not in punctuation)

def load_library():
    print('Loading library...', end=' ')
    global original_library, clean_library
    original_library = {verbatim : code for verbatim, code in mssql_engine.execute('select verbatim, code from open_library').fetchall()}
    clean_library = {clean_verbatim(verbatim): code for verbatim, code in original_library.items() if clean_verbatim(verbatim)}

    print('OK')

def empty_open_tables():

    print('Emptying open tables...', end=' ')
    mssql_engine.execute(f'delete from open_tokenized where {settings.serial_criteria}')
    mssql_engine.execute(f'delete from open_coded where {settings.serial_criteria}')
    print('OK')

def tokenize_answers():
    
    print('Tokenizing answers...', end=' ')
    for serial, variable, answer in mssql_engine.execute(f'''
        select o.serial, o.variable, o.answer
        from import_open as o
            join open_variables as v
                on o.variable = v.variable
        where {settings.serial_criteria}
            and o.answer like '%' + char(10) + '%'
            and v.type = 'brand'
        ''').fetchall():
        tokenized_answers = answer.split('\n')
        for index, tokenized_answer in enumerate(tokenized_answers):
            if tokenized_answer:
                mssql_engine.execute(f'''
                    insert into open_tokenized
                    values({serial}, '{variable}', {index}, char(10), '{tokenized_answer.replace("'", "''")}')
                    ''')
    print('OK')

def lookup(func, func_name, library):

    print(f'Coding using {func_name}...', end=' ')
    records = []
    for serial, variable, position, answer in mssql_engine.execute(f'''
        select serial, variable, position, answer
        from open_brands_uncoded
        where {settings.serial_criteria}
        ''').fetchall():
        clean_answer = func(answer)
        if clean_answer in library:
            record = f'''({serial}, '{variable}', {position}, '{answer}', '{func_name}', {library[clean_answer]}, 1, '{clean_answer}')'''
            records.append(record)
    write_records(records, 'open_coded')
    print('OK')

def lev():

    print(f'Coding using levenshtein...', end=' ')
    records = []
    for serial, variable, position, answer in mssql_engine.execute(f'''
        select serial, variable, position, answer
        from open_brands_uncoded
        where {settings.serial_criteria}
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
    print('OK')

def svss():
    print(f'Coding using svss...', end=' ')
    delete = ['ich weiss nicht', 'ich weiß nicht', 'ich weis nicht',
               'ich weiß nicht was', 'ich weiß', 'ich hatte bin', 'ich habe bin']
    records = []
    for serial, variable, position, answer in mssql_engine.execute(f'''
        select serial, variable, position, answer
        from open_brands_uncoded
        where {settings.serial_criteria}
        ''').fetchall():
        clean_answer = clean_verbatim(answer)
        if clean_answer in delete:
            record = f'''({serial}, '{variable}', {position}, '{answer}', 'svss', 0, 1, '{clean_answer}')'''
            records.append(record)
        if len(clean_answer) < 2:
            record = f'''({serial}, '{variable}', {position}, '{answer}', 'svss_len', 0, 1, '')'''
            records.append(record)

    write_records(records, 'open_coded')
    print('OK')

def bigramms():
    
    print('starting bigramms...', end=' ')
    df = pd.read_sql(
        f'select serial, variable, position, answer from open_brands_uncoded where {settings.serial_criteria}', mssql_engine)
    verbatims = df.to_dict('list')
    verbatims['func'] = []
    verbatims['code'] = []
    verbatims['score'] = []
    verbatims['candidate'] = []

    bigramms = ['bc', 'bf', 'bj', 'bx', 'cb', 'cf', 'cg', 'cj', 'cq', 'cv', 'cx', 'cy', 'dq', 'dx', 'fc', 'fj', 'fq', 'fx', 'fy', 'gc', 'gq', 'gx', 'gy', 'hx', 'jb', 'jc', 'jd', 'jg', 'jh', 'ji', 'jj', 'jk', 'jl', 'jm', 'jn', 'jp', 'jq', 'jr', 'js', 'jt', 'jv', 'jw', 'jx', 'jy', 'jz', 'kj', 'kx', 'lq', 'mq', 'mx', 'nx', 'oq', 'pj', 'px', 'qa', 'qb', 'qc', 'qd', 'qe', 'qf', 'qg', 'qh', 'qj', 'qk', 'qm', 'qp', 'qq', 'qr', 'qs', 'qt', 'qv', 'qw', 'qx', 'qy', 'qz', 'rq', 'rx', 'sq', 'sx', 'tx', 'uj', 'uy', 'vb', 'vc', 'vf', 'vg', 'vj', 'vn', 'vq', 'vx', 'vy', 'wq', 'wx', 'wy', 'wz', 'xb', 'xc', 'xd', 'xf', 'xg', 'xh', 'xj', 'xk', 'xl', 'xm', 'xn', 'xq', 'xr', 'xs', 'xv', 'xw', 'xy', 'xz', 'yf', 'yg', 'yh', 'yi', 'yj', 'yk', 'yq', 'yt', 'yu', 'yv', 'yx', 'yy', 'yz', 'zf', 'zh', 'zj', 'zr', 'zx', 'zy', 'aj', 'bd', 'bh', 'bn', 'by', 'dj', 'eq', 'fh', 'fk', 'fp', 'fv', 'fw', 'gj', 'hc', 'hj', 'ij', 'kg', 'kh', 'km', 'kn', 'kz', 'lj', 'oj', 'uu', 'uz', 'wj', 'ws', 'xt', 'ys', 'zd', 'zg', 'zm', 'zz', 'cd', 'dh', 'fb', 'fn', 'gf', 'hf', 'jo', 'kd', 'lh', 'mj', 'sj', 'tj', 'ui', 'uw', 'aw', 'bg', 'df', 'dz', 'ej', 'fg', 'gb', 'gg', 'gh', 'gw', 'gz', 'hh', 'hk', 'hz', 'iu', 'pd']

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

    df_coded.to_sql('open_coded', mssql_engine, index=False, if_exists='append')

    print('OK')

def repeats():
    print('starting repeats...', end=' ')
    df = pd.read_sql(
        f'select serial, variable, position, answer from open_brands_uncoded where {settings.serial_criteria}', mssql_engine)
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

    df_coded.to_sql('open_coded', mssql_engine, index=False, if_exists='append')

    print('OK')

def numbers():
    
    print('starting numbers...', end=' ')
    df = pd.read_sql(
        f'select serial, variable, position, answer from open_brands_uncoded where {settings.serial_criteria}', mssql_engine)
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

    df_coded.to_sql('open_coded', mssql_engine, index=False, if_exists='append')

    print('OK')

def ml_neuronet():
    print('starting ml...', end=' ')
    with open('neuronet_2018-06-21.pkl', 'rb') as f:
        mlp_nn = pickle.load(f)

    PUNCTUATIONS = list(punctuation)

    df = pd.read_sql(
        f'select serial, variable, position, answer from open_brands_uncoded where {settings.serial_criteria}', mssql_engine)
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

def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))

def write_records(records, table_name):
    insert_header = '''
        insert into {0}
        values 
    '''.format(table_name)
    for batch in chunker(records, 1000):
        insert_values = ','.join(batch)
        insert_statement = insert_header + insert_values
        mssql_cursor.execute(insert_statement)
        mssql_conn.commit()

def main():
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
