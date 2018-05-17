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

class Interview(Base):
    __tablename__ = 'live_interviews_mroledb'

    serial = Column(Integer, primary_key=True)
    fbgrot = Column(Unicode(50), nullable=False)
    start_time = Column(DateTime, nullable=False)
    age = Column(Integer, nullable=False)
    fa = Column(Unicode(50), nullable=False)
    fb = Column(Unicode(50), nullable=False)
    fc = Column(Unicode(50), nullable=False)
    ff = Column(Unicode(50), nullable=False)
    fg = Column(Unicode(50), nullable=False)
    fha = Column(Unicode(50), nullable=False)
    fhb = Column(Unicode(50), nullable=True)
    f12a = Column(Unicode(50), nullable=False)
    f12b = Column(Unicode(50), nullable=True)
    weight = Column(Float, nullable=False)

    def __repr__(self):
        return '<Interview(serial={0})>'.format(self.serial)

print('{0}: setting up orm'.format(datetime.now()))

mssql_engine = create_engine(mssql_connection)
Base.metadata.create_all(mssql_engine)
session = Session(bind=mssql_engine)

print('{0}: deleting interviews'.format(datetime.now()))
session.execute('delete from live_interviews_mroledb')

print('{0}: querying ddf'.format(datetime.now()))

ddf = client.Dispatch('ADODB.Connection')
ddf.ConnectionString = ddf_connection
ddf.Open()
rs, _ = ddf.Execute('''
    select
        Respondent.Serial as serial, fbgrot,
        DataCollection.StartTime as start_time,
        f$$ as age, fa, fb, fc, ff, fg,
        fha, fhb, f12a, f12b, weight
    from VDATA''')

def clean_value(rs_field):
    if rs_field.Value:
        return ''.join(c for c in rs_field.Value if c not in '{}')
    else:
        return None

print('{0}: looping rs'.format(datetime.now()))

while not rs.EOF:
    i = Interview(
        serial = rs.Fields['serial'].Value,
        fbgrot = clean_value(rs.Fields['fbgrot']),
        start_time = rs.Fields['start_time'].Value,
        age = rs.Fields['age'].Value,
        fa = clean_value(rs.Fields['fa']),
        fb = clean_value(rs.Fields['fb']),
        fc = clean_value(rs.Fields['fc']),
        ff = clean_value(rs.Fields['ff']),
        fg = clean_value(rs.Fields['fg']),
        fha = clean_value(rs.Fields['fha']),
        fhb = clean_value(rs.Fields['fhb']),
        f12a = clean_value(rs.Fields['f12a']),
        f12b = clean_value(rs.Fields['f12b']),
        weight = rs.Fields['weight'].Value
    )
    session.add(i)
    rs.MoveNext()

ddf.Close()
print('{0}: committing'.format(datetime.now()))

session.commit()

print('{0}: complete'.format(datetime.now()))
