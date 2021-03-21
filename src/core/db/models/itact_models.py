# coding: utf-8
from sqlalchemy import BigInteger, Column, Integer, String, text
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class Migration(Base):
    __tablename__ = 'migrations'

    id = Column(Integer, primary_key=True, server_default=text("nextval('migrations_id_seq'::regclass)"))
    timestamp = Column(BigInteger, nullable=False)
    name = Column(String, nullable=False)
