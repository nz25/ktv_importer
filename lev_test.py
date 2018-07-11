
# pylint: disable-msg=e1101

from string import punctuation, whitespace
import re
from collections import OrderedDict, defaultdict, deque
from sqlalchemy import create_engine
import Levenshtein as lv

from load_data import chunker, write_records
from settings import MSSQL_CONNECTION

mssql_engine = create_engine(MSSQL_CONNECTION)
mssql_conn = mssql_engine.connect().connection
mssql_cursor = mssql_conn.cursor()
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
    
def lev_test():
    print(f'Coding using levenshtein...', end=' ')
    records = []
    idx = 0

    for serial, variable, position, answer in mssql_engine.execute(f'''
        select serial, variable, position, answer
        from lev_test
        ''').fetchall():
        clean_answer = clean_verbatim(answer)
        answer_results = []
        for verbatim, code in clean_library.items():
            score = lv.ratio(clean_answer, verbatim)
            if score > 0:
                answer_results.append((score, verbatim, code))

        answer_results.sort(key=lambda x: x[0], reverse=True)

        current_code = -1
        best_matches = []
        for score, verbatim, code in answer_results:
            if len(best_matches) == 2:
                break
            if current_code != code:
                best_matches.append(f'''({serial}, '{variable}', {position}, '{clean_answer.replace("'", r"''")}', {len(best_matches)}, {score}, '{verbatim.replace("'", r"''")}', {code})''')
                current_code = code

        records.extend(best_matches)

        idx += 1
        if idx % 10000 == 0:
            print(f'{idx} respondents processed')

    write_records(records, 'lev_test_results')
    print(f'{len(records)} records')

def clean_up():
    print('Cleaning up')
    mssql_engine.execute('delete from lev_test_results')

clean_up()
load_library()
lev_test()