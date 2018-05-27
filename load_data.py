import sqlite3
import xml.etree.cElementTree as et
from sqlalchemy import create_engine

from timeit import timeit

mdd_path = 'D:\\ktv\\KTVONLINE_18_IMPORT.mdd'
ddf_path = 'D:\\ktv\\KTVONLINE_18_IMPORT.ddf'
category_map = {}
records = []

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
    mssql_cursor.execute('delete from import_interviews')
    mssql_conn.commit()
    print('OK')


def read_import_interviews():
    print('Reading interviews...', end=' ')

    global records
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
        record = (
            row[0],  # serial
            category_map[row[1]],  # fbgrot
            row[2],  # start_time
            row[3],  # age
            category_map[row[4]],  # fa
            category_map[row[5]],  # fb
            category_map[row[6]],  # fc
            category_map[row[7]],  # ff
            category_map[row[8]],  # fg
            category_map[row[9]],  # fha
            category_map.get(row[10], 'na'),  # fhb
            category_map[row[11]],  # f12a
            category_map.get(row[12], 'na'),  # f12b
            row[13]  # weight
        )
        stringified_record = str(record)
        records.append(stringified_record)
    print('OK')


def chunker(seq, size):
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


def write_import_interviews():
    print('Inserting interviews...', end=' ')
    insert_header = '''
        insert into import_interviews
        (serial, fbgrot, start_time, age, fa, fb, fc, ff, fg, fha, fhb, f12a, f12b, weight)
        values 
    '''
    for batch in chunker(records, 1000):
        insert_values = ','.join(batch)
        insert_statement = insert_header + insert_values
        mssql_cursor.execute(insert_statement)
        mssql_conn.commit()
    print('OK')


def main():
    read_category_map()
    empty_import_tables()
    read_import_interviews()
    write_import_interviews()
    print('Complete')


if __name__ == '__main__':
    print(timeit(main, number=1))
