# create_output.py

# pylint: disable-msg=w0614

from settings import *

import pandas as pd
from sqlalchemy import create_engine

engine = create_engine(mssql_connection)

def verbaco_input():
    uncoded = pd.read_sql("select serial, variable, answer from open_uncoded", engine)

    for i in range(len(uncoded)):
        if "'" in uncoded.loc[(i, 'answer')]:
            uncoded.loc[(i, 'answer')] = uncoded.loc[(i, 'answer')].replace("'", r"\'")

    with open ('verbaco_input.txt', 'w', encoding='utf-16') as f:
        for i in range(len(uncoded)):
            serial = str(uncoded.loc[(i, 'serial')])
            question = str(uncoded.loc[(i, 'variable')])
            answer = str(uncoded.loc[(i, 'answer')])
            if len(answer) > 3000:
                answer = answer[:3000]
            if i == 0:
                start = '##recstart ' + serial  + '\n'
                f.write(start)
            else:
                if uncoded.loc[(i, 'serial')] != uncoded.loc[(i-1, 'serial')]:
                    end = '##end ' + str(uncoded.loc[(i-1, 'serial')]) + '\n'
                    start = '##recstart ' + serial  + '\n'
                    f.write(end)
                    f.write(start)
            line = '##v ' +"'" + question + "'='" + answer + "'\n"
            f.write(line)
        end = '##end ' + str(uncoded.loc[(i, 'serial')]) + '\n'
        f.write(end)
        

def coding():
    coded = pd.read_sql("select * from open_coded", engine)
    writer = pd.ExcelWriter('Coded.xlsx', engine='xlsxwriter')
    coded.to_excel(writer,'Sheet1', index = False)


    

def main():
    verbaco_input()
    coding()

if __name__ == '__main__':
    main()




    
