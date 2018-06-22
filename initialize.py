
# prepare_data.py

import settings
from settings import raw_mdd_path, raw_ddf_path, \
    mdd_path, ddf_path, mssql_connection, wave

from shutil import copyfile
from os import path
from sqlalchemy import create_engine
from win32com import client
from xml.etree import cElementTree


def backup():
    print('Backing up...', end=' ')

    copyfile(raw_mdd_path, mdd_path)
    copyfile(raw_ddf_path, ddf_path)

    mdd = client.Dispatch('MDM.Document')
    mdd.Open(mdd_path)
    mdd.DataSources.Default.DBLocation = ddf_path
    mdd.Save()
    mdd.Close()
    print('OK')

def read_increment():
    print('Reading increment...', end=' ')
    mssql_engine = create_engine(mssql_connection)
    result = mssql_engine.execute(
        'select increment from waves where wave_id = ?', wave
        ).fetchone()
    settings.serial_increment = result['increment']
    settings.serial_criteria = f'''serial between {str(settings.serial_increment * 1_000_000)}
            and {str((settings.serial_increment + 1) * 1_000_000 - 1)}'''
    print('OK')
    
def read_category_map():
    print('Reading category map...', end=' ')
    tree = cElementTree.parse(mdd_path)
    map_root = tree.getroot()[0].find('categorymap')
    settings.category_map = {int(m.attrib['value']): m.attrib['name'] for m in map_root}
    print('OK')

def main():
    if path.exists(raw_mdd_path):
        backup()
        read_increment()
        read_category_map()
        print('Initialization complete\n')
    else:
        directory, file_name = path.split(raw_mdd_path)
        raise FileNotFoundError(f'Source file {file_name} not found in {directory}')

if __name__ == '__main__':
    main()