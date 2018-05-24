# load_data.py

# pylint: disable-msg=w0614

from settings import *

from win32com import client

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy import MetaData, Table, Column
from sqlalchemy import Integer, DateTime, Unicode, Float

from datetime import datetime     

Base = declarative_base()

class Verbatim(Base):
    __tablename__ = 'verbatims'

    serial = Column(Integer, primary_key=True)
    question = Column(Unicode(50), primary_key=True)
    iteration = Column(Integer, primary_key=True)
    answer = Column(Unicode(255), nullable=False)

    def __repr__(self):
        return '<Interview(serial={0})>'.format(self.serial)

print('{0}: setting up orm'.format(datetime.now()))

mssql_engine = create_engine(mssql_connection)
Base.metadata.create_all(mssql_engine)
session = Session(bind=mssql_engine)

print('{0}: deleting verbatims'.format(datetime.now()))
session.execute('delete from verbatims')

def clean_value(rs_field, filter_out):
    return ''.join(c for c in rs_field.Value if c not in filter_out) if rs_field.Value else None

print('{0}: querying ddf'.format(datetime.now()))

ddf = client.Dispatch('ADODB.Connection')
ddf.ConnectionString = ddf_connection
ddf.Open()
rs, _ = ddf.Execute('''
    select ^.Respondent.Serial as serial,
        1 as question, LevelId as iteration, f1 as answer
    from hdata.f1l
    where f1 <> ''
    ''')

print('{0}: looping f1'.format(datetime.now()))

while not rs.EOF:
    v = Verbatim(
        serial = rs.Fields['serial'].Value,
        start_time = rs.Fields['start_time'].Value,
        question = rs.Fields['question'].Value,
        iteration = clean_value(rs.Fields['iteration'], 'c_{}'),
        answer = rs.Fields['answer'].Value
    )
    session.add(v)
    rs.MoveNext()

rs, _ = ddf.Execute('''
    select ^.Respondent.Serial as serial,
        2 as question, LevelId as iteration, f2 as answer
    from hdata.f2l
    where f2 <> ''
    ''')

print('{0}: looping f2'.format(datetime.now()))

while not rs.EOF:
    v = Verbatim(
        serial = rs.Fields['serial'].Value,
        start_time = rs.Fields['start_time'].Value,
        question = rs.Fields['question'].Value,
        iteration = clean_value(rs.Fields['iteration'], 'c_{}'),
        answer = rs.Fields['answer'].Value
    )
    session.add(v)
    rs.MoveNext()

ddf.Close()
print('{0}: committing'.format(datetime.now()))

session.commit()

print('{0}: complete'.format(datetime.now()))
