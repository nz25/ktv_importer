
# prepare_data.py

import settings
from settings import RAW_MDD_PATH, RAW_DDF_PATH, DP_MDD_PATH, \
    DP_DDF_PATH, MDD_PATH, DDF_PATH, MSSQL_CONNECTION, WAVE

from shutil import copyfile
from os import path
from sqlalchemy import create_engine
from win32com import client
from xml.etree import cElementTree


def backup():
    print('Backing up...', end=' ')

    copyfile(RAW_MDD_PATH, MDD_PATH)
    copyfile(RAW_DDF_PATH, DDF_PATH)

    copyfile(RAW_MDD_PATH, DP_MDD_PATH)
    copyfile(RAW_DDF_PATH, DP_DDF_PATH)

    mdd = client.Dispatch('MDM.Document')
    mdd.Open(MDD_PATH)
    mdd.DataSources.Default.DBLocation = DDF_PATH
    mdd.Save()
    mdd.Close()
    print('OK')

def read_increment():
    print('Reading increment...', end=' ')
    mssql_engine = create_engine(MSSQL_CONNECTION)
    result = mssql_engine.execute(
        'select increment from waves where wave_id = ?', WAVE
        ).fetchone()
    settings.SERIAL_INCREMENT = result['increment']
    settings.SERIAL_CRITERIA = f'''serial between {settings.SERIAL_INCREMENT * 1_000_000}
            and {(settings.SERIAL_INCREMENT + 1) * 1_000_000 - 1}'''
    print('OK')
    
def read_category_map():
    print('Reading category map...', end=' ')
    tree = cElementTree.parse(MDD_PATH)
    map_root = tree.getroot()[0].find('categorymap')
    settings.CATEGORY_MAP = {int(m.attrib['value']): m.attrib['name'] for m in map_root}
    print('OK')

def main():
    print('INITIALIZATION')
    if path.exists(RAW_MDD_PATH):
        backup()
        read_increment()
        read_category_map()
        print('Initialization complete', end='\n\n')
    else:
        directory, file_name = path.split(RAW_MDD_PATH)
        raise FileNotFoundError(f'Source file {file_name} not found in {directory}')

if __name__ == '__main__':
    main()