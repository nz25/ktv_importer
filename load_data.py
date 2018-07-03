# load_data.py

import settings
from settings import MDD_PATH, DDF_PATH, SQLITE_CONNECTION, \
    MSSQL_CONNECTION, MROLEDB_CONNECTION
from enums import openConstants, DataTypeConstants
from win32com import client
import sqlite3
from sqlalchemy import create_engine
from collections import defaultdict

# sqlite - source db
sqlite_conn = sqlite3.connect(SQLITE_CONNECTION)
sqlite_cursor = sqlite_conn.cursor()

# ms sql - destination db
mssql_engine = create_engine(MSSQL_CONNECTION)
mssql_conn = mssql_engine.connect().connection
mssql_cursor = mssql_conn.cursor()

ddf = client.Dispatch('ADODB.Connection')
ddf.ConnectionString = MROLEDB_CONNECTION

def empty_import_tables():
    print('Emptying destination tables...', end=' ')

    mssql_cursor.execute(f'delete from import_open where {settings.SERIAL_CRITERIA}')
    mssql_cursor.execute(f'delete from import_fc where {settings.SERIAL_CRITERIA}')
    mssql_cursor.execute(f'delete from import_funnel where {settings.SERIAL_CRITERIA}')
    mssql_cursor.execute(f'delete from import_interviews where {settings.SERIAL_CRITERIA}')
    mssql_conn.commit()
    print('OK')

def read_import_interviews():
    print('Reading interviews...', end=' ')

    records = []

    result = sqlite_cursor.execute('''
        select
            [Respondent.Serial:L] as serial,
            [fbgrot:C1] as fbgrot,
            [DataCollection.StartTime:T] as start_time,
            [f$$:L] as age,
            [fa:C1] as fa,
            [fb:C1] as fb,
            [fc:C1] as fc,
            [ff:C1] as ff,
            [fg:C1] as fg,
            [fha:C1] as fha,
            [fhb:C1] as fhb,
            [f12a:C1] as f12a,
            [f12b:C1] as f12b,
            [weight:D] as weight
        from L1
        ''')
    for row in result:
        serial = row[0] + settings.SERIAL_INCREMENT * 1_000_000
        fbgrot = settings.CATEGORY_MAP[row[1]]
        # compensates for difference between dimensions float->datetime and
        # sql server float->datetime conversions (difference is 2 days after February 1900)
        start_time = row[2] - 2
        age = row[3]
        fa = settings.CATEGORY_MAP[row[4]]
        fb = settings.CATEGORY_MAP[row[5]]
        fc = settings.CATEGORY_MAP[row[6]]
        ff = settings.CATEGORY_MAP[row[7]]
        fg = settings.CATEGORY_MAP[row[8]]
        fha = settings.CATEGORY_MAP[row[9]]
        fhb = settings.CATEGORY_MAP.get(row[10], 'na')
        f12a = settings.CATEGORY_MAP[row[11]]
        f12b = settings.CATEGORY_MAP.get(row[12], 'na')
        weight = row[13]

        record = str((serial, fbgrot, start_time, age,
            fa, fb, fc, ff, fg, fha, fhb,
            f12a, f12b, weight))
        records.append(record)

    write_records(records, 'import_interviews')
    print(f'{len(records)} records')

def read_import_funnel():
    print('Reading funnel questions...', end=' ')

    questions = 'f3g f5g f7g'.split()
    records = []

    result = sqlite_cursor.execute('''
        select [Respondent.Serial:L],
            [f3g:S], [f5g:S], [f7g:S]
        from L1
        ''')
    for row in result:
        serial = row[0] + settings.SERIAL_INCREMENT * 1_000_000
        for index, question in enumerate(questions, start=1):
            brands = row[index]
            if brands:
                for brand_value in brands.split(';'):
                    if brand_value:
                        brand = settings.CATEGORY_MAP.get(int(brand_value))
                        record = str((serial, question, brand))
                        records.append(record)
    
    write_records(records, 'import_funnel')
    print(f'{len(records)} records')

def read_import_fc():
    print('Reading fc...', end=' ')

    records = []

    tables = sqlite_cursor.execute('''
        select TableName, DSCTableName
        from Levels
        where DSCTableName in ('f8l')
        ''')
    table_name = tables.fetchone()[0]
    
    result = sqlite_cursor.execute('''
        select b.[Respondent.Serial:L], a.[LevelId:C1], a.[f8:C1]
        from {0} as a
            join L1 as b
                on a.[:P1] = b.[:P0]
        where a.[f8:C1] > 0
        '''.format(table_name))

    for row in result:
        serial = row[0] + settings.SERIAL_INCREMENT * 1_000_000
        category = settings.CATEGORY_MAP[row[1]]
        brand = settings.CATEGORY_MAP[row[2]]
        record = str((serial, category, brand))
        records.append(record)
        
    write_records(records, 'import_fc')
    print(f'{len(records)} records')

def read_import_open_vdata():
    print('Reading open questions...', end=' ')

    # reading ignored variables from db
    ignored_variables = [v[0] for v in mssql_engine.execute('select variable from open_variables where type = ?', 'ignored').fetchall()]

    # reading all text variables from mdd
    mdm_variables = []
    mdd = client.Dispatch('MDM.Document')
    mdd.Open(MDD_PATH,mode=openConstants.oREAD)
    for v in mdd.Variables:
        if v.DataType == DataTypeConstants.mtText and not v.IsSystemVariable and v.HasCaseData:
            if not v.FullName in ignored_variables:
                mdm_variables.append(v.FullName)
    mdd.Close()
    
    ddf.Open()
    rs, _ = ddf.Execute('select Respondent.Serial, {0} from vdata'.format(','.join(mdm_variables)))

    records = []
    rs.MoveFirst()
    while not rs.EOF:
        for f in rs.Fields:
            answer = None
            if f.Name == 'Respondent.Serial':
                serial = int(f.Value) + settings.SERIAL_INCREMENT * 1_000_000
            else:
                answer = f.Value
            if answer:
                variable = f.Name
                answer = answer.replace("'", "''")
                record = '(' + str(serial) + ", '" + variable + "', '" + answer + "')"
                records.append(record)
        rs.MoveNext()

    ddf.Close()
       
    write_records(records, 'import_open')
    print(f'{len(records)} records')

def read_import_open():
    print('Reading open questions...', end = ' ')

    # reading levels 
    levels = {l[0] : [l[1], [l[2]]] for l in sqlite_cursor.execute('''
        select TableName, DSCTableName, ParentName
        from Levels''').fetchall()}
    for v in levels.values():
        parent_tables = v[1]
        parent_table = parent_tables[0]
        while parent_table:
            parent_table = levels[parent_table][1][0]
            parent_tables.append(parent_table)
    levels = {k: [v[0], v[1][:-1]] for k, v in levels.items()}

    # # reading ignored variables from db
    # ignored_variables = set(v[0] for v in mssql_engine.execute('''select variable
    #     from open_variables where type = ?''', 'ignored').fetchall())

    # reading all text variables from mdd and builds flat list (VDATA) of variables
    flat_variables = []
    mdd = client.Dispatch('MDM.Document')
    mdd.Open(MDD_PATH,mode=openConstants.oREAD)
    for v in mdd.Variables:
        # if v.DataType == DataTypeConstants.mtText and not v.IsSystemVariable \
        # and v.HasCaseData and v.FullName not in ignored_variables:
        if v.DataType == DataTypeConstants.mtText and v.HasCaseData:
            flat_variables.append(v.FullName)
    mdd.Close()

    # build a dictionary of variables with corresponding level and field name on that level
    # e.g. f1l[{c_1}].f1 -> f1l.f1
    variables = {}
    for fv in flat_variables:
        v = '.'.join(part.split('[')[0] for part in fv.split('.'))
        variables[v] = ['L1', '']
   
    # assign levels to variable
    for va in variables:
        variables[va][1] = va
        parts = va.split('.')        
        for i in range(len(parts) - 1):
            prefix = parts[i]
            suffix = '.'.join(parts[i + 1:])
            for k, v in levels.items():
                if prefix == v[0]:
                    variables[va][0] = k
                    variables[va][1] = suffix
                    break
        
    # build tables dictionary containing list of text variables in this table
    tables = defaultdict(list)
    for v, l in variables.items():
        tables[l[0]].append(l[1])
    
    # relevant tables: tables containing text variables + their parent tables
    relevant_tables = {}
    for t in tables.keys():
        relevant_tables.update({l : {} for l in levels[t][1] + [t]})

    for t in relevant_tables.keys():
        all_columns = [r[1] for r in sqlite_cursor.execute(f'pragma table_info({t})').fetchall()]
        id_columns = [f'[{c}]' for c in all_columns if c[:2] == ':P']
        if 'LevelId:C1' in all_columns:
            values_dict = {row[:-1]: row[-1] for row in sqlite_cursor.execute(f'''
                select {','.join(id_columns)}, [LevelId:C1]
                from {t}''').fetchall()}
            relevant_tables[t] = values_dict

    # builds sql queries per table
    records = []
    for t in tables:
        # get column list
        all_columns = sqlite_cursor.execute(f'pragma table_info({t})').fetchall()
        id_columns = [f'[{c[1]}]' for c in all_columns if c[1][:2] == ':P']
        text_columns = [f'[{c}:X]' for c in tables[t]]
        query_columns = id_columns + text_columns

        # level_tables contains the table itself and all its parents
        # number of id columns should match number of level tables
        level_tables = levels[t][1][::-1] + [t]
        assert len(id_columns) == len(level_tables)
        
        #builds and executes sql
        for row in sqlite_cursor.execute(f'''select {','.join(query_columns)} from {t}'''):
            serial = row[0] + settings.SERIAL_INCREMENT * 1_000_000
            variable_prefix = ''
            for i in range(1, len(level_tables)):
                field = levels[level_tables[i]][0]
                iteration_value = relevant_tables[level_tables[i]][row[:i + 1]]
                iteration_category = settings.CATEGORY_MAP[iteration_value]
                variable_prefix += f'{field}[{{{iteration_category}}}].'
            for i in range(len(text_columns)):    
                offset = i + len(id_columns)
                variable = variable_prefix + text_columns[i][1:-3]
                answer = row[offset]
                if answer:
                    answer = answer.replace("'", "''")
                    record = '(' + str(serial) + ", '" + variable + "', '" + answer + "')"
                    records.append(record)
       
    write_records(records, 'import_open')
    print(f'{len(records)} records')
    
def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))

def write_records(records, table_name):
    insert_header = f'''
        insert into {table_name}
        values
        '''
    for batch in chunker(records, 1000):
        insert_values = ','.join(batch)
        insert_statement = insert_header + insert_values
        mssql_cursor.execute(insert_statement)
        mssql_conn.commit()

def main():
    print('DATA LOADING')
    empty_import_tables()
    read_import_interviews()
    read_import_funnel()
    read_import_fc()
    read_import_open()

    print('Data loading complete', end='\n\n')

if __name__ == '__main__':
    import initialize
    import prepare_data
    initialize.main()
    prepare_data.main()

    from timeit import timeit
    print(timeit(main,number=1))