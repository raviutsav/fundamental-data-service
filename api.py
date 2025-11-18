from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from typing import List, Optional
from sqlalchemy import create_engine, Column, BigInteger, String, Enum, Numeric, ForeignKey
from sqlalchemy.orm import sessionmaker, declarative_base, Session, relationship
import enum
import os
from dotenv import load_dotenv

load_dotenv()

HOST = os.getenv("HOST")
DATABASE = os.getenv("DATABASE")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
PORT = os.getenv("PORT")

# ---------------------------------------------------------------------------
# Database Setup
# ---------------------------------------------------------------------------
DATABASE_URL = f"postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# ---------------------------------------------------------------------------
# ENUMS matching PostgreSQL
# ---------------------------------------------------------------------------
class FinancialStatementType(str, enum.Enum):
    per_share_data_array = "per_share_data_array"
    common_size_ratios = "common_size_ratios"
    income_statement = "income_statement"
    balance_sheet = "balance_sheet"
    cashflow_statement = "cashflow_statement"
    valuation_ratios = "valuation_ratios"
    valuation_and_quality = "valuation_and_quality"
    other = "other"

class PeriodType(str, enum.Enum):
    annuals = "annuals"
    quarterly = "quarterly"

# ---------------------------------------------------------------------------
# SQLAlchemy Models
# ---------------------------------------------------------------------------
class FundamentalDataType(Base):
    __tablename__ = "fundamental_data_type"

    id = Column(BigInteger, primary_key=True, index=True)
    type = Column(Enum(FinancialStatementType), nullable=False)
    name = Column(String(50), nullable=False)

class FundamentalData(Base):
    __tablename__ = "fundamental_data"

    id = Column(BigInteger, primary_key=True, index=True)
    ticker = Column(String(30), nullable=False)
    period = Column(Enum(PeriodType), nullable=False)
    year = Column(String(5), nullable=False)
    month = Column(String(3), nullable=False)
    fundamental_data_type_id = Column(BigInteger, ForeignKey("fundamental_data_type.id", ondelete="CASCADE"), nullable=False)
    value = Column(Numeric, nullable=False)

    data_type = relationship("FundamentalDataType")

# ---------------------------------------------------------------------------
# Pydantic Schemas
# ---------------------------------------------------------------------------
class FundamentalDataTypeSchema(BaseModel):
    id: int
    type: FinancialStatementType
    name: str

    class Config:
        orm_mode = True

class FundamentalDataSchema(BaseModel):
    id: int
    ticker: str
    period: PeriodType
    year: str
    month: str
    fundamental_data_type_id: int
    value: float
    data_type: FundamentalDataTypeSchema

    class Config:
        orm_mode = True

# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI()

# Dependency

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ---------------------------------------------------------------------------
# API Endpoints
# ---------------------------------------------------------------------------

# Fetch all fundamental data types
@app.get("/fundamental-data-types", response_model=List[FundamentalDataTypeSchema])
def get_fundamental_data_types(db: Session = Depends(get_db)):
    return db.query(FundamentalDataType).all()

# Fetch fundamental data by ticker
@app.get("/fundamental-data/{ticker}", response_model=List[FundamentalDataSchema])
def get_data_by_ticker(
    ticker: str,
    period: Optional[PeriodType] = None,
    year: Optional[str] = None,
    month: Optional[str] = None,
    statement_type: Optional[FinancialStatementType] = None,  # ENUM type filter
    name: Optional[str] = None,  # data type name filter
    db: Session = Depends(get_db)
):
    query = (
        db.query(FundamentalData)
        .join(FundamentalDataType)
        .filter(FundamentalData.ticker == ticker)
    )

    if period:
        query = query.filter(FundamentalData.period == period)

    if year:
        query = query.filter(FundamentalData.year == year)

    if month:
        query = query.filter(FundamentalData.month == month)

    if statement_type:
        query = query.filter(FundamentalDataType.type == statement_type)

    if name:
        query = query.filter(FundamentalDataType.name == name)

    result = query.all()

    if not result:
        raise HTTPException(status_code=404, detail="No data found for this ticker")

    return result


# Fetch all unique tickers
@app.get("/tickers", response_model=List[str])
def get_all_tickers(db: Session = Depends(get_db)):
    tickers = db.query(FundamentalData.ticker).distinct().all()
    return [t[0] for t in tickers]

@app.get("/ticker/{ticker}/availability")
def get_ticker_availability(ticker: str, db: Session = Depends(get_db)):
    rows = (
        db.query(FundamentalData.period, FundamentalData.year, FundamentalData.month)
        .filter(FundamentalData.ticker == ticker)
        .distinct()  # ensure unique combinations
        .all()
    )

    if not rows:
        raise HTTPException(status_code=404, detail="No data found for this ticker")

    availability = {}
    unique_set = set()  # to ensure uniqueness at Python level too

    for period, year, month in rows:
        key = (period, year, month)

        if key in unique_set:
            continue

        unique_set.add(key)

        if period not in availability:
            availability[period] = []

        availability[period].append({
            "year": year,
            "month": month
        })

    return availability


# ---------------------------------------------------------------------------
# END OF FILE
# ---------------------------------------------------------------------------
