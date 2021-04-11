# coding: utf-8
from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Integer,
    String,
    text,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class DocumentType(Base):
    __tablename__ = "document_type"

    id = Column(UUID, primary_key=True, server_default=text("uuid_generate_v4()"))
    name = Column(String, nullable=False)


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


class ComplianceRunEvent(Base):
    __tablename__ = "compliance_run_event"

    id = Column(UUID, primary_key=True)
    created_at = Column(DateTime, nullable=False, server_default=text("now()"))
    updated_at = Column(DateTime, nullable=False, server_default=text("now()"))
    deleted_at = Column(DateTime)
    s3_bucket = Column(String, nullable=False)
    s3_key = Column(String, nullable=False)
    was_redacted = Column(Boolean, nullable=False)
    status = Column(String, nullable=False)
    document_type_id = Column(ForeignKey("document_type.id"))

    document_type = relationship("DocumentType")


class EmployeeToComplianceRunEvent(Base):
    __tablename__ = "employee_to_compliance_run_event"

    id = Column(UUID, primary_key=True, server_default=text("uuid_generate_v4()"))
    employee_id = Column(ForeignKey("employee.id"), nullable=False)
    compliance_run_event_id = Column(
        ForeignKey("compliance_run_event.id"), nullable=False
    )

    compliance_run_event = relationship("ComplianceRunEvent")
    employee = relationship("Employee")


class Fincen8300Rev4(Base):
    __tablename__ = "fincen8300_rev4"

    id = Column(UUID, primary_key=True, server_default=text("uuid_generate_v4()"))
    amends_prior_report = Column(String)
    suspicious_transaction = Column(String)
    multiple_individuals = Column(String)
    last_name = Column(String)
    first_name = Column(String)
    middle_initial = Column(String)
    tin = Column(String)
    address = Column(String)
    dob = Column(Date)
    city = Column(String)
    state = Column(String)
    zip = Column(String)
    country = Column(String)
    occ_prof_bus = Column(String)
    identdoc_describe = Column(String)
    identdoc_issued_by = Column(String)
    identdoc_number = Column(String)
    multiple_behalf = Column(String)
    last_name_org_name_behalf = Column(String)
    first_name_behalf = Column(String)
    middle_initial_behalf = Column(String)
    tin_behalf = Column(String)
    dba_behalf = Column(String)
    address_behalf = Column(String)
    occ_prof_bus_behalf = Column(String)
    city_behalf = Column(String)
    state_behalf = Column(String)
    zip_behalf = Column(String)
    country_behalf = Column(String)
    identdoc_describe_behalf = Column(String)
    identdoc_issued_by_behalf = Column(String)
    identdoc_number_behalf = Column(String)
    date_cash_received = Column(Date)
    total_cash_received = Column(String)
    cash_received_mult_pmts = Column(String)
    total_price = Column(String)
    usde_us_currency = Column(String)
    usde_amt_benj_higher = Column(String)
    usde_foreign_currency = Column(String)
    foreign_currency_country = Column(String)
    usde_cashiers_checks = Column(String)
    usde_money_orders = Column(String)
    usde_bank_drafts = Column(String)
    usde_travelers_checks = Column(String)
    type_ot_personal_prop = Column(String)
    type_ot_real_property = Column(String)
    type_ot_personal_services = Column(String)
    type_ot_business_services = Column(String)
    type_ot_intangible_property = Column(String)
    type_ot_debt_obligations_paid = Column(String)
    type_ot_exchange_of_cash = Column(String)
    type_ot_escrow_or_trust_funds = Column(String)
    type_ot_bail_received = Column(String)
    type_ot_other = Column(String)
    specific_description_of_property = Column(String)
    business_name_brc = Column(String)
    ein_brc = Column(String)
    address_brc = Column(String)
    city_brc = Column(String)
    state_brc = Column(String)
    zip_code_brc = Column(String)
    nature_of_business_brc = Column(String)
    signature_footer = Column(String)
    title_footer = Column(String)
    date_of_signature_footer = Column(Date)
    contact_name_printed_brc = Column(String)
    contact_phone_brc = Column(String)
    compliance_run_event_id = Column(ForeignKey("compliance_run_event.id"), unique=True)

    compliance_run_event = relationship("ComplianceRunEvent", uselist=False)
