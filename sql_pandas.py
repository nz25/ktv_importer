# pylint: disable-msg=w0614

from settings import *

import pandas as pd
from sqlalchemy import create_engine

engine = create_engine(mssql_connection)

engine.execute('''
IF OBJECT_ID('dbo.test_table', 'U') IS NOT NULL 
  DROP TABLE dbo.test_table 
''')




df = pd.read_sql('select * from verbatims', engine)


#df.to_sql('test_table', engine)