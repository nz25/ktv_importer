# load_data.py

# pylint: disable-msg=w0614

from settings import *

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from sqlalchemy import MetaData, Table, Column
from sqlalchemy import Integer, DateTime, Unicode, Float

import sqlite3
#from prepare_data import category_map, read_category_map

from datetime import datetime     

Base = declarative_base()

class Interview(Base):
    __tablename__ = 'live_interviews'

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

msssql_engine = create_engine(mssql_connection)
Base.metadata.create_all(msssql_engine)
session = Session(bind=msssql_engine)

print('{0}: deleting interviews'.format(datetime.now()))
session.execute('delete from live_interviews_sqlite')

print('{0}: querying ddf'.format(datetime.now()))

conn = sqlite3.connect(dest + '.ddf')
cursor = conn.cursor()
result = cursor.execute('''
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

print('{0}: reading categorymap'.format(datetime.now()))
read_category_map()

#1. using orm

print('{0}: looping rs'.format(datetime.now()))

for row in result:
    i = Interview(
        serial = row[0],
        fbgrot = category_map.get(row[1]),
        start_time = row[2],
        age = row[3],
        fa = category_map.get(row[4]),
        fb = category_map.get(row[5]),
        fc = category_map.get(row[6]),
        ff = category_map.get(row[7]),
        fg = category_map.get(row[8]),
        fha = category_map.get(row[9]),
        fhb = category_map.get(row[10]),
        f12a = category_map.get(row[11]),
        f12b = category_map.get(row[12]),
        weight = row[13]
    )
    session.add(i)
    
print('{0}: committing'.format(datetime.now()))

session.commit()

print('{0}: complete'.format(datetime.now()))
