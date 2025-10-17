from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from datetime import datetime

from src.database import get_db
from src import schemas
from src.services import TaxiTripService

router = APIRouter()


# --- CRUD TRAJETS ---

@router.get("/trips", response_model=schemas.TaxiTripList, tags=["Trips"])
def get_trips(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    trips, total = TaxiTripService.get_trips(db, skip=skip, limit=limit)
    return schemas.TaxiTripList(total=total, trips=trips)


@router.get("/trips/{trip_id}", response_model=schemas.TaxiTrip, tags=["Trips"])
def get_trip(trip_id: int, db: Session = Depends(get_db)):
    trip = TaxiTripService.get_trip(db, trip_id)
    if not trip:
        raise HTTPException(status_code=404, detail="Trip not found")
    return trip


@router.post("/trips", response_model=schemas.TaxiTrip, tags=["Trips"])
def create_trip(trip: schemas.TaxiTripCreate, db: Session = Depends(get_db)):
    return TaxiTripService.create_trip(db, trip)


@router.put("/trips/{trip_id}", response_model=schemas.TaxiTrip, tags=["Trips"])
def update_trip(trip_id: int, trip: schemas.TaxiTripUpdate, db: Session = Depends(get_db)):
    updated = TaxiTripService.update_trip(db, trip_id, trip)
    if not updated:
        raise HTTPException(status_code=404, detail="Trip not found")
    return updated


@router.delete("/trips/{trip_id}", tags=["Trips"])
def delete_trip(trip_id: int, db: Session = Depends(get_db)):
    deleted = TaxiTripService.delete_trip(db, trip_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Trip not found")
    return {"message": f"Trip {trip_id} deleted successfully"}


# --- STATISTIQUES ---

@router.get("/statistics", response_model=schemas.Statistics, tags=["Statistics"])
def get_statistics(db: Session = Depends(get_db)):
    stats = TaxiTripService.get_statistics(db)
    return stats


# --- PIPELINE ---

@router.post("/pipeline/run", response_model=schemas.PipelineResponse, tags=["Pipeline"])
def run_pipeline(db: Session = Depends(get_db)):
    """
    Simulation d’un pipeline de téléchargement et import CSV.
    Dans un vrai cas, cette fonction appellerait une tâche ETL.
    """
    # Exemple simulé
    file_name = "yellow_tripdata_sample.csv"
    rows_imported = 1500
    import_date = datetime.utcnow()

    # Enregistrement dans la table ImportLog
    from src.models import ImportLog
    log = ImportLog(file_name=file_name, import_date=import_date, rows_imported=rows_imported)
    db.add(log)
    db.commit()

    return schemas.PipelineResponse(
        file_name=file_name,
        import_date=import_date,
        rows_imported=rows_imported
    )
