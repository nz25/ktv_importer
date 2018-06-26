# create_output.py

from settings import MSSQL_CONNECTION, DAU_LOCATION
from sqlalchemy import create_engine

mssql_engine = create_engine(MSSQL_CONNECTION)

def write_dau():
    print('Writing dau file...', end=' ')
    records = [row for row in mssql_engine.execute(f'''
        select serial, variable, answer
        from open_uncoded
        order by serial, variable, answer
        ''').fetchall()]

    with open(DAU_LOCATION, 'w', encoding='utf-16') as f:
        current_serial = 0
        for record in records:
            serial = record[0]
            variable = record[1]
            answer = clean_answer(record[2])
            if serial != current_serial:
                if current_serial:
                    f.write(f'##end {current_serial}\n')
                f.write(f'##recstart {serial}\n')
                current_serial = serial
            f.write(f"##v '{variable}'='{answer}'\n")
        f.write(f'##end {current_serial}\n')
    print('OK')

def clean_answer(verbatim):
    clean = verbatim[:3000]
    clean = clean.replace('\n', ' ').replace('\r', ' ')
    clean = clean.replace("'", "\'")
    return clean

def main():
    print('OUTPUTS CREATION')
    write_dau()
    print('Output creation complete')

if __name__ == '__main__':
    main()




    
