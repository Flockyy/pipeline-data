from pydantic import BaseModel, ConfigDict
from datetime import datetime
from typing import Optional, List


# Base Schemas


class TaxiTripBase(BaseModel):
    vendor_id: Optional[str] = None
    pickup_datetime: Optional[datetime] = None
    dropoff_datetime: Optional[datetime] = None
    passenger_count: Optional[int] = None
    trip_distance: Optional[float] = None
    ratecode_id: Optional[int] = None
    store_and_fwd_flag: Optional[str] = None
    pu_location_id: Optional[int] = None
    do_location_id: Optional[int] = None
    payment_type: Optional[int] = None
    fare_amount: Optional[float] = None
    extra: Optional[float] = None
    mta_tax: Optional[float] = None
    tip_amount: Optional[float] = None
    tolls_amount: Optional[float] = None
    improvement_surcharge: Optional[float] = None
    total_amount: Optional[float] = None
    congestion_surcharge: Optional[float] = None
    airport_fee: Optional[float] = None


class TaxiTripCreate(TaxiTripBase):
    pass


class TaxiTripUpdate(TaxiTripBase):
    pass


class TaxiTrip(TaxiTripBase):
    id: int
    model_config = ConfigDict(from_attributes=True)


class TaxiTripList(BaseModel):
    total: int
    trips: List[TaxiTrip]


class Statistics(BaseModel):
    total_trips: int
    earliest_trip: Optional[datetime]
    latest_trip: Optional[datetime]
    average_fare: Optional[float]
    average_distance: Optional[float]


class PipelineResponse(BaseModel):
    file_name: str
    rows_imported: int
    import_date: datetime
