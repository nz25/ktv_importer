import sqlite3
import xml.etree.cElementTree as et
from sqlalchemy import create_engine

from timeit import timeit

mdd_path = 'D:\\ktv\\KTVONLINE_18_IMPORT.mdd'
ddf_path = 'D:\\ktv\\KTVONLINE_18_IMPORT.ddf'
category_map = {}

# sqlite - source db
sqlite_conn = sqlite3.connect(ddf_path)
sqlite_cursor = sqlite_conn.cursor()

# ms sql - destination db
mssql_connection = 'mssql+pyodbc://./dw_04_live?driver=SQL+Server+Native+Client+11.0?trusted_connection=yes'
mssql_engine = create_engine(mssql_connection)
mssql_conn = mssql_engine.connect().connection
mssql_cursor = mssql_conn.cursor()

def read_category_map():
    print('Reading category map...', end=' ')
    tree = et.parse(mdd_path)
    map_root = tree.getroot()[0].find('categorymap')
    global category_map
    category_map = {int(m.attrib['value']): m.attrib['name'] for m in map_root}
    print('OK')

def empty_import_tables():
    print('Emptying destination tables...', end=' ')
    mssql_cursor.execute('delete from import_open')
    mssql_cursor.execute('delete from import_funnel')
    mssql_cursor.execute('delete from import_interviews')
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
        serial = row[0]
        fbgrot = category_map[row[1]]
        start_time = row[2]
        age = row[3]
        fa = category_map[row[4]]
        fb = category_map[row[5]]
        fc = category_map[row[6]]
        ff = category_map[row[7]]
        fg = category_map[row[8]]
        fha = category_map[row[9]]
        fhb = category_map.get(row[10], 'na')
        f12a = category_map[row[11]]
        f12b = category_map.get(row[12], 'na')
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
        serial = row[0]
        for index, question in enumerate(questions, start=1):
            brands = row[index]
            if brands:
                for brand_value in brands.split(';'):
                    if brand_value:
                        brand = category_map.get(int(brand_value))
                        record = str((serial, question, brand))
                        records.append(record)
    print('OK')
    return records


def read_import_open():
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
            select [:P1], [LevelId:C1], [{0}:X]
            from {1}
            where [{0}:X] <> ''
            '''.format(question, table_name))
        for row in result:
            serial = row[0]
            iteration = int(category_map[row[1]][2:])
            answer = row[2]
            record = str((serial, question, iteration, answer))
            records.append(record)
        
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
    read_category_map()
    empty_import_tables()
    interviews = read_import_interviews()
    funnel = read_import_funnel()
    opens = read_import_open()

    write_records(interviews, 'import_interviews')
    write_records(funnel, 'import_funnel')
    write_records(opens, 'import_open')
    print('Complete')


if __name__ == '__main__':
    print(timeit(main, number=1))
