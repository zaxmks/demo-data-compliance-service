# coding: utf-8
from sqlalchemy import BigInteger, Column, Integer, String, text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class Fincen8300Rev4(Base):
    __tablename__ = 'fincen8300_rev4'

    id = Column(UUID, primary_key=True, server_default=text("uuid_generate_v4()"))
    amends_prior_report = Column(String, nullable=False)
    suspicious_transaction = Column(String, nullable=False)
    multiple_individuals = Column(String, nullable=False)
    last_name = Column(String, nullable=False)
    first_name = Column(String, nullable=False)
    middle_initial = Column(String, nullable=False)
    tin = Column(String, nullable=False)
    address = Column(String, nullable=False)
    dob = Column(String, nullable=False)
    city = Column(String, nullable=False)
    state = Column(String, nullable=False)
    zip = Column(String, nullable=False)
    country = Column(String, nullable=False)
    occ_prof_bus = Column(String, nullable=False)
    identdoc_describe = Column(String, nullable=False)
    identdoc_issued_by = Column(String, nullable=False)
    identdoc_number = Column(String, nullable=False)
    multiple_behalf = Column(String, nullable=False)
    last_name_org_name_behalf = Column(String, nullable=False)
    first_name_behalf = Column(String, nullable=False)
    middle_initial_behalf = Column(String, nullable=False)
    tin_behalf = Column(String, nullable=False)
    dba_behalf = Column(String, nullable=False)
    address_behalf = Column(String, nullable=False)
    occ_prof_bus_behalf = Column(String, nullable=False)
    city_behalf = Column(String, nullable=False)
    state_behalf = Column(String, nullable=False)
    zip_behalf = Column(String, nullable=False)
    country_behalf = Column(String, nullable=False)
    identdoc_describe_behalf = Column(String, nullable=False)
    identdoc_issued_by_behalf = Column(String, nullable=False)
    identdoc_number_behalf = Column(String, nullable=False)
    date_cash_received = Column(String, nullable=False)
    total_cash_received = Column(String, nullable=False)
    cash_received_mult_pmts = Column(String, nullable=False)
    total_price = Column(String, nullable=False)
    usde_us_currency = Column(String, nullable=False)
    usde_amt_benj_higher = Column(String, nullable=False)
    usde_foreign_currency = Column(String, nullable=False)
    foreign_currency_country = Column(String, nullable=False)
    usde_cashiers_checks = Column(String, nullable=False)
    usde_money_orders = Column(String, nullable=False)
    usde_bank_drafts = Column(String, nullable=False)
    usde_travelers_checks = Column(String, nullable=False)
    type_ot_personal_prop = Column(String, nullable=False)
    type_ot_real_property = Column(String, nullable=False)
    type_ot_personal_services = Column(String, nullable=False)
    type_ot_business_services = Column(String, nullable=False)
    type_ot_intangible_property = Column(String, nullable=False)
    type_ot_debt_obligations_paid = Column(String, nullable=False)
    type_ot_exchange_of_cash = Column(String, nullable=False)
    type_ot_escrow_or_trust_funds = Column(String, nullable=False)
    type_ot_bail_received = Column(String, nullable=False)
    type_ot_other = Column(String, nullable=False)
    specific_description_of_property = Column(String, nullable=False)
    business_name_brc = Column(String, nullable=False)
    ein_brc = Column(String, nullable=False)
    address_brc = Column(String, nullable=False)
    city_brc = Column(String, nullable=False)
    state_brc = Column(String, nullable=False)
    zip_code_brc = Column(String, nullable=False)
    nature_of_business_brc = Column(String, nullable=False)
    signature_footer = Column(String, nullable=False)
    title_footer = Column(String, nullable=False)
    date_of_signature_footer = Column(String, nullable=False)
    contact_name_printed_brc = Column(String, nullable=False)
    contact_phone_brc = Column(String, nullable=False)


class Migration(Base):
    __tablename__ = 'migrations'

    id = Column(Integer, primary_key=True, server_default=text("nextval('migrations_id_seq'::regclass)"))
    timestamp = Column(BigInteger, nullable=False)
    name = Column(String, nullable=False)

