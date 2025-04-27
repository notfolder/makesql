from sqlalchemy import Column, String, Integer, Float
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class ValueTable(Base):
    __tablename__ = 'value_table'
    
    id = Column(Integer, primary_key=True)
    serial = Column(String(9))
    serial_sub = Column(String(2))
    x = Column(Integer)
    y = Column(Integer)
    attr_name = Column(String)
    attr_value = Column(Float)

class SummaryTable(Base):
    __tablename__ = 'summary_table'
    
    id = Column(Integer, primary_key=True)
    serial = Column(String(9))
    serial_sub = Column(String(2))
    sum_type = Column(String)
    attr_name = Column(String)
    attr_value = Column(Float)