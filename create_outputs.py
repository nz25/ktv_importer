# create_outputs.py

import settings
from settings import MSSQL_CONNECTION, DAU_LOCATION, CFILE_LOCATION
from sqlalchemy import create_engine
from collections import defaultdict

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

def write_cfile():
    print('Writing c-file...', end=' ')

    with open(CFILE_LOCATION, 'w', encoding='utf-8') as f:

        serials = defaultdict(list)

        for serial, variable, position, code in mssql_engine.execute(f'''
                select serial, variable, position, code
                from open_coded_current_wave
                where {settings.SERIAL_CRITERIA}
                    and code <> 0
                order by serial, variable, position
                ''').fetchall():
            serials[serial].append((variable, position, code))
        
        for serial, data in serials.items():
            variables = defaultdict(list)
            variables_sql = []
            for variable, position, code in data:
                variables[variable].append(f'cb_{code}')
            for variable, codes in variables.items():
                #remove duplicates from codes
                codes = list(dict.fromkeys(codes))
                variables_sql.append(f'{variable}.Coding={{{",".join(codes)}}}')
            sql = f'update vdata set {",".join(variables_sql)} where respondent.serial = {serial}\n'
            f.write(sql)

        # for serial, data in serials.items():
        #     variables = defaultdict(list)
        #     for variable, position, code in data:
        #         variables[variable].append(f'cb_{code}')
        #     for variable, codes in variables.items():
        #         #remove duplicates from codes
        #         codes = list(dict.fromkeys(codes))
        #         sql = f'update vdata set {variable}.Coding={{{",".join(codes)}}} where respondent.serial = {serial}\n'
        #         f.write(sql)

    print('OK')

def main():
    print('OUTPUTS CREATION')
    write_dau()
    write_cfile()
    print('Output creation complete', end='\n\n')

if __name__ == '__main__':
    import initialize
    initialize.main()
    main()