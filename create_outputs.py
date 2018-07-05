# create_outputs.py

import settings
from settings import MSSQL_CONNECTION, DAU_LOCATION
from sqlalchemy import create_engine

mssql_engine = create_engine(MSSQL_CONNECTION)

def write_dau():
    print('Writing dau file...', end=' ')

    with open(DAU_LOCATION, 'w', encoding='utf-16') as f:
        current_serial = 0
        for serial, variable, answer in mssql_engine.execute(f'''
            select serial, variable, answer
            from open_uncoded
            where {settings.SERIAL_CRITERIA}
            order by serial, variable, answer
            ''').fetchall():
            answer = clean_answer(answer)
            if serial != current_serial:
                if current_serial:
                    f.write(f'##end {current_serial}\n')
                f.write(f'##recstart {serial}\n')
                current_serial = serial
            f.write(f"##v '{variable}' = '{answer}'\n")
        f.write(f'##end {current_serial}')
    print('OK')

def clean_answer(verbatim):
    clean = verbatim[:3000]
    clean = clean.replace('\n', ' ').replace('\r', ' ')
    clean = clean.replace("'", r"\'")
    return clean

def main():
    print('OUTPUTS CREATION')
    write_dau()
    print('Output creation complete', end='\n\n')

if __name__ == '__main__':
    import initialize
    initialize.main()
    main()