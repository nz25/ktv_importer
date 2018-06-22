# load_data.py

import settings
from settings import mdd_path, ddf_path, sqlite_connection, \
    mssql_connection, mroledb_connection
from enums import openConstants, DataTypeConstants
from win32com import client
import sqlite3
from sqlalchemy import create_engine

# sqlite - source db
sqlite_conn = sqlite3.connect(sqlite_connection)
sqlite_cursor = sqlite_conn.cursor()

# ms sql - destination db
mssql_engine = create_engine(mssql_connection)
mssql_conn = mssql_engine.connect().connection
mssql_cursor = mssql_conn.cursor()

ddf = client.Dispatch('ADODB.Connection')
ddf.ConnectionString = mroledb_connection

def empty_import_tables():
    print('Emptying destination tables...', end=' ')

    mssql_cursor.execute(f'delete from import_open where {settings.serial_criteria}')
    mssql_cursor.execute(f'delete from import_fc where {settings.serial_criteria}')
    mssql_cursor.execute(f'delete from import_funnel where {settings.serial_criteria}')
    mssql_cursor.execute(f'delete from import_interviews where {settings.serial_criteria}')
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
        serial = row[0] + settings.serial_increment * 1_000_000
        fbgrot = settings.category_map[row[1]]
        start_time = row[2]
        age = row[3]
        fa = settings.category_map[row[4]]
        fb = settings.category_map[row[5]]
        fc = settings.category_map[row[6]]
        ff = settings.category_map[row[7]]
        fg = settings.category_map[row[8]]
        fha = settings.category_map[row[9]]
        fhb = settings.category_map.get(row[10], 'na')
        f12a = settings.category_map[row[11]]
        f12b = settings.category_map.get(row[12], 'na')
        weight = row[13]

        record = str((serial, fbgrot, start_time, age,
            fa, fb, fc, ff, fg, fha, fhb,
            f12a, f12b, weight))
        records.append(record)
    print('OK')
    return records

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
        serial = row[0] + settings.serial_increment * 1000000
        for index, question in enumerate(questions, start=1):
            brands = row[index]
            if brands:
                for brand_value in brands.split(';'):
                    if brand_value:
                        brand = settings.category_map.get(int(brand_value))
                        record = str((serial, question, brand))
                        records.append(record)
    print('OK')
    return records

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
        serial = row[0] + settings.serial_increment * 1000000
        category = settings.category_map[row[1]]
        brand = settings.category_map[row[2]]
        record = str((serial, category, brand))
        records.append(record)
        
    print('OK')
    return records

def read_import_open_old():
    print('Reading open questions...', end=' ')

    questions = 'f1l f2l'.split()
    records = []

    tables = sqlite_cursor.execute('''
        select TableName, DSCTableName
        from Levels
        where DSCTableName in ({0})
    '''.format(','.join("'" + q + "'" for q in questions)))

    for table in tables:
        table_name = table[0]
        question = table[1][:-1]
        result = sqlite_cursor.execute('''
            select b.[Respondent.Serial:L], a.[LevelId:C1], a.[{0}:X]
            from {1} as a
                join L1 as b
                    on a.[:P1] = b.[:P0]
            where a.[{0}:X] <> ''
            '''.format(question, table_name))
        for row in result:
            serial = row[0] + settings.serial_increment * 1000000
            question = table[1] + '[{' + settings.category_map[row[1]] + '}].' + table[1][:-1]
            answer = row[2].replace("'", "#####")
            record = str((serial, question, answer)).replace("#####", "''")
            records.append(record)
        
    print('OK')
    return records

def read_import_open():
    print('Reading open questions...', end=' ')

    # reading ignored variables from db
    ignored_variables = [v[0] for v in mssql_engine.execute('select variable from open_variables where type = ?', 'ignored').fetchall()]

    # reading all text variables from mdd
    mdm_variables = []
    mdd = client.Dispatch('MDM.Document')
    mdd.Open(mdd_path,mode=openConstants.oREAD)
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
                serial = int(f.Value) + settings.serial_increment * 1000000
            else:
                answer = f.Value
            if answer:
                variable = f.Name
                answer = answer.replace("'", "''")
                record = '(' + str(serial) + ", '" + variable + "', '" + answer + "')"
                records.append(record)
        rs.MoveNext()

    ddf.Close()
        
    print('OK')
    return records

def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))

def write_records(records, table_name):
    print('Inserting into {0}...'.format(table_name), end=' ')
    insert_header = '''
        insert into {0}
        values 
    '''.format(table_name)
    for batch in chunker(records, 1000):
        insert_values = ','.join(batch)
        insert_statement = insert_header + insert_values
        mssql_cursor.execute(insert_statement)
        mssql_conn.commit()
    print('OK')

def main():
    empty_import_tables()
    interviews = read_import_interviews()
    funnel = read_import_funnel()
    fc = read_import_fc()
    opens = read_import_open()

    write_records(interviews, 'import_interviews')
    write_records(funnel, 'import_funnel')
    write_records(fc, 'import_fc')
    write_records(opens, 'import_open')
    print('Data loading complete', end='\n\n')

if __name__ == '__main__':
    import initialize
    import prepare_data
    initialize.main()
    prepare_data.main()
    main()