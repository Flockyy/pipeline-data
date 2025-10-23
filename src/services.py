from sqlalchemy.orm import Session
from sqlalchemy import func
from src import models, schemas


class TaxiTripService:
    @staticmethod
    def get_trip(db: Session, trip_id: int):
        """Retrieve a trip by its ID"""
        return db.query(models.YellowTaxiTrip).filter(models.YellowTaxiTrip.id == trip_id).first()

    @staticmethod
    def get_trips(db: Session, skip: int = 0, limit: int = 100):
        """Retrieve a paginated list of trips"""
        query = db.query(models.YellowTaxiTrip)
        total = query.count()
        trips = query.offset(skip).limit(limit).all()
        return trips, total

    @staticmethod
    def create_trip(db: Session, trip: schemas.TaxiTripCreate):
        """Create a new trip"""
        db_trip = models.YellowTaxiTrip(**trip.dict())
        db.add(db_trip)
        db.commit()
        db.refresh(db_trip)
        return db_trip

    @staticmethod
    def update_trip(db: Session, trip_id: int, trip: schemas.TaxiTripUpdate):
        """Update an existing trip"""
        db_trip = db.query(models.YellowTaxiTrip).filter(models.YellowTaxiTrip.id == trip_id).first()
        if not db_trip:
            return None
        for key, value in trip.dict(exclude_unset=True).items():
            setattr(db_trip, key, value)
        db.commit()
        db.refresh(db_trip)
        return db_trip

    @staticmethod
    def delete_trip(db: Session, trip_id: int):
        """Delete a trip"""
        db_trip = db.query(models.YellowTaxiTrip).filter(models.YellowTaxiTrip.id == trip_id).first()
        if not db_trip:
            return False
        db.delete(db_trip)
        db.commit()
        return True

    @staticmethod
    def get_statistics(db: Session):
        """Compute trip statistics"""
        total_trips = db.query(func.count(models.YellowTaxiTrip.id)).scalar()
        earliest_trip = db.query(func.min(models.YellowTaxiTrip.pickup_datetime)).scalar()
        latest_trip = db.query(func.max(models.YellowTaxiTrip.dropoff_datetime)).scalar()
        average_fare = db.query(func.avg(models.YellowTaxiTrip.fare_amount)).scalar()
        average_distance = db.query(func.avg(models.YellowTaxiTrip.trip_distance)).scalar()

        return schemas.Statistics(
            total_trips=total_trips or 0,
            earliest_trip=earliest_trip,
            latest_trip=latest_trip,
            average_fare=average_fare,
            average_distance=average_distance,
        )