from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from datetime import datetime
import logging
import traceback

from src.database import get_db
from src import schemas
from src.models import ImportLog
from src.dlt_pipeline import NYCTaxiDLTPipeline  # adjust import if needed
from src.services import TaxiTripService

router = APIRouter()

# --- CRUD: Taxi Trips ---

@router.get("/trips", response_model=schemas.TaxiTripList, tags=["Trips"])
def get_trips(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    """
    Retrieve a paginated list of taxi trips.
    - `skip`: number of records to skip (for pagination)
    - `limit`: number of records to return
    """
    trips, total = TaxiTripService.get_trips(db, skip=skip, limit=limit)
    return schemas.TaxiTripList(total=total, trips=trips)


@router.get("/trips/{trip_id}", response_model=schemas.TaxiTrip, tags=["Trips"])
def get_trip(trip_id: int, db: Session = Depends(get_db)):
    """
    Retrieve a single trip by its unique ID.
    Raises a 404 error if the trip does not exist.
    """
    trip = TaxiTripService.get_trip(db, trip_id)
    if not trip:
        raise HTTPException(status_code=404, detail="Trip not found")
    return trip


@router.post("/trips", response_model=schemas.TaxiTrip, tags=["Trips"])
def create_trip(trip: schemas.TaxiTripCreate, db: Session = Depends(get_db)):
    """
    Create a new taxi trip record.
    Expects a JSON body matching the TaxiTripCreate schema.
    """
    return TaxiTripService.create_trip(db, trip)


@router.put("/trips/{trip_id}", response_model=schemas.TaxiTrip, tags=["Trips"])
def update_trip(trip_id: int, trip: schemas.TaxiTripUpdate, db: Session = Depends(get_db)):
    """
    Update an existing taxi trip record.
    Raises a 404 error if the trip ID does not exist.
    """
    updated = TaxiTripService.update_trip(db, trip_id, trip)
    if not updated:
        raise HTTPException(status_code=404, detail="Trip not found")
    return updated


@router.delete("/trips/{trip_id}", tags=["Trips"])
def delete_trip(trip_id: int, db: Session = Depends(get_db)):
    """
    Delete a taxi trip by its ID.
    Returns a success message if deletion is successful,
    or raises 404 if the trip is not found.
    """
    deleted = TaxiTripService.delete_trip(db, trip_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Trip not found")
    return {"message": f"Trip {trip_id} deleted successfully"}


# --- STATISTICS ---

@router.get("/statistics", response_model=schemas.Statistics, tags=["Statistics"])
def get_statistics(db: Session = Depends(get_db)):
    """
    Retrieve aggregated statistics on taxi trips.
    This could include metrics such as:
    - total trips
    - average trip distance
    - average fare amount
    etc.
    """
    stats = TaxiTripService.get_statistics(db)
    return stats

# --- PIPELINE ---

@router.post("/pipeline/run", response_model=schemas.PipelineResponse, tags=["Pipeline"])
def run_pipeline(background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """
    Trigger the NYC Taxi DLT pipeline asynchronously (non-blocking).
    The API returns immediately while the ETL runs in a background task.
    """

    import_date = datetime.utcnow()
    file_name = f"nyc_taxi_pipeline_{import_date:%Y%m%d_%H%M%S}"

    # Create a placeholder log entry for this pipeline run
    log = ImportLog(
        file_name=file_name,
        import_date=import_date,
        rows_imported=0,
        status="running",
        message="Pipeline started..."
    )
    db.add(log)
    db.commit()
    db.refresh(log)

    # Define the background job
    def background_job(log_id: int):
        session = None
        try:
            from src.database import SessionLocal
            session = SessionLocal()

            pipeline = NYCTaxiDLTPipeline()
            load_info = pipeline.run_pipeline(destination="postgres")

            rows_imported = getattr(load_info, "rows_imported", 0)
            message = str(load_info)

            log_entry = session.query(ImportLog).filter(ImportLog.id == log_id).first()
            if log_entry:
                log_entry.rows_imported = rows_imported
                log_entry.status = "completed"
                log_entry.message = message[:5000]
                log_entry.completed_at = datetime.utcnow()
                session.commit()

        except Exception as e:
            logging.error("Pipeline execution failed", exc_info=True)
            if session:
                log_entry = session.query(ImportLog).filter(ImportLog.id == log_id).first()
                if log_entry:
                    log_entry.status = "failed"
                    log_entry.message = f"{type(e).__name__}: {str(e)}\n{traceback.format_exc()[:4000]}"
                    log_entry.completed_at = datetime.utcnow()
                    session.commit()
        finally:
            if session:
                session.close()

    # Run background job asynchronously
    background_tasks.add_task(background_job, log.id)

    # Return immediate response
    return schemas.PipelineResponse(
        file_name=file_name,
        import_date=import_date,
        rows_imported=0,
        message="Pipeline started asynchronously.",
        status="running"
    )
