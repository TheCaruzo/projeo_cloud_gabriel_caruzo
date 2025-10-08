# verificar se vale fazer via ORM.
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, DECIMAL

Base = declarative_base()

class PregaoReport(Base):
    __tablename__ = 'pregao_report'
    id = Column(Integer, primary_key=True, autoincrement=True)
    ativo = Column(String, nullable=False)
    data_pregao = Column(Date, nullable=False)
    abertura = Column(DECIMAL(10, 2), nullable=True)
    fechamento = Column(DECIMAL(10, 2), nullable=True)
    preco_minimo = Column(DECIMAL(10, 2), nullable=True)
    preco_maximo = Column(DECIMAL(10, 2), nullable=True)
    total_volume = Column(DECIMAL(20, 2), nullable=True)