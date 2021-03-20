from src.core.db.db_names import DatabaseEnum
from src.core.db.models.pdf_models import Fincen8300Rev4
from src.core.db.session import DBContext

def demo_db():
    with DBContext(DatabaseEnum.PDF_INGESTION_DB) as pdf_db:
        pdf_db.add(Fincen8300Rev4(first_name='Tom'))


if __name__ == '__main__':
    demo_db()