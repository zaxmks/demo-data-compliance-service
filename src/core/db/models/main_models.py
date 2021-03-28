# coding: utf-8
from sqlalchemy import BigInteger, Column, Date, ForeignKey, Integer, String, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class Employee(Base):
    __tablename__ = "employee"

    id = Column(UUID, primary_key=True, server_default=text("uuid_generate_v4()"))
    prefix_name = Column(String)
    first_name = Column(String, nullable=False)
    last_name = Column(String, nullable=False)
    middle_name = Column(String)
    suffix_name = Column(String)
    date_of_birth = Column(Date)
    dod_id = Column(String)
    ssn = Column(String)


class Migration(Base):
    __tablename__ = "migrations"

    id = Column(
        Integer,
        primary_key=True,
        server_default=text("nextval('migrations_id_seq'::regclass)"),
    )
    timestamp = Column(BigInteger, nullable=False)
    name = Column(String, nullable=False)


class EmployeeToDocument(Base):
    __tablename__ = "employee_to_document"

    id = Column(UUID, primary_key=True, server_default=text("uuid_generate_v4()"))
    employee_id = Column(UUID, nullable=False)
    ingestion_event_id = Column(UUID, nullable=False)
    related_employee_id = Column(ForeignKey("employee.id"))

    related_employee = relationship("Employee")
