from pydantic import BaseModel

class DSManifestTrip(BaseModel):
    trip_id: str
    sub_trip: int
    health_grade: int = 500


class DSManifest(BaseModel):
    request_id: str
    trips: list[DSManifestTrip]
