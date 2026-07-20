from pydantic import BaseModel

class DSManifestTrip(BaseModel):
    trip_id: str
    sub_trip: int


class DSManifest(BaseModel):
    request_id: str
    trips: list[DSManifestTrip]
