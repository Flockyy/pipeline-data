from sqlalchemy import Column, Integer, String, Float, DateTime, BigInteger
from datetime import datetime
from src.database import Base

class YellowTaxiTrip(Base):
    __tablename__ = "yellow_taxi_trips"

    id = Column(BigInteger, primary_key=True, index=True)
    vendor_id = Column(String, nullable=True)
    pickup_datetime = Column(DateTime, nullable=True)
    dropoff_datetime = Column(DateTime, nullable=True)
    passenger_count = Column(Integer, nullable=True)
    trip_distance = Column(Float, nullable=True)
    ratecode_id = Column(Integer, nullable=True)
    store_and_fwd_flag = Column(String, nullable=True)
    pu_location_id = Column(Integer, nullable=True)
    do_location_id = Column(Integer, nullable=True)
    payment_type = Column(Integer, nullable=True)
    fare_amount = Column(Float, nullable=True)
    extra = Column(Float, nullable=True)
    mta_tax = Column(Float, nullable=True)
    tip_amount = Column(Float, nullable=True)
    tolls_amount = Column(Float, nullable=True)
    improvement_surcharge = Column(Float, nullable=True)
    total_amount = Column(Float, nullable=True)
    congestion_surcharge = Column(Float, nullable=True)
    airport_fee = Column(Float, nullable=True)


class ImportLog(Base):
    __tablename__ = "import_log"

    id = Column(Integer, primary_key=True, index=True)
    file_name = Column(String, nullable=False)
    import_date = Column(DateTime, default=datetime.utcnow, nullable=False)
    completed_at = Column(DateTime, nullable=True)
    rows_imported = Column(Integer, default=0, nullable=False)
    status = Column(String, default="pending", nullable=False)
    message = Column(String, nullable=True)
