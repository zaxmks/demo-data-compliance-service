from src.core.db.config import DatabaseEnum
from src.core.db.models.pdf_models import Fincen8300Rev4
from src.core.db.session import DBContext

f = Fincen8300Rev4(first_name='Tom')
print(f)

models = [
    Fincen8300Rev4(first_name='Tom'),
    Fincen8300Rev4(first_name='Sally'),
]
with DBContext(DatabaseEnum.PDF_INGESTION_DB) as pdf_db:
    pdf_db.add(Fincen8300Rev4(first_name='Tom'))