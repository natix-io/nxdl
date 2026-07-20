from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field


class FootageSEIFormat(StrEnum):
    default = "default"


class TripCSVFormat(StrEnum):
    default = "default"


class TripJSONFormat(StrEnum):
    default = "default"


class TripMCAPFormat(StrEnum):
    default = "default"


class FootageCSVFormat(StrEnum):
    default = "default"
    here_tech = "here_tech"


class FootageMCAPFormat(StrEnum):
    default = "default"


class PhoneMetadataOptions(StrEnum):
    any = "any"
    with_metadata = "with"
    without = "without"


class SEIDataOptions(StrEnum):
    any = "any"
    with_data = "with"
    without = "without"
    all_data = "all"


class SafetyCriticalEventsFilter(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    names: list[str] | None = None
    minEventCount: int = 1


class EgoActionsFilter(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    names: list[str] | None = None
    minEventCount: int = 1


class Range(BaseModel):
    """Numeric inclusive range."""

    min: int | None = Field(default=None, description="Minimum value (inclusive)")
    max: int | None = Field(default=None, description="Maximum value (inclusive)")


class DatasetSettings(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    name: str = Field(description="Name of the dataset used as the base folder name")
    client_ids: list[str] | None = Field(default_factory=list)
    trip_label_threshold_percent: int = Field(default=50)
    anonymize: bool = Field(default=False)


class Region(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    continent: str | None = Field(default=None)
    countries: list[str] | None = Field(default_factory=list)
    exclude_countries: list[str] | None = Field(default_factory=list)
    states: list[str] | None = Field(default_factory=list)
    exclude_states: list[str] | None = Field(default_factory=list)
    cities: list[str] | None = Field(default_factory=list)
    hours: int = Field(default=10)


class Trips(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    ids: list[str] | None = Field(default_factory=list)
    duration: Range | None = Field(default_factory=lambda: Range(min=1, max=600))
    tripStart: Range | None = Field(default=None)
    perUser: int | None = Field(default=None)
    distance: Range | None = Field(default=None)
    avgSpeed: Range | None = Field(default=None)
    phoneMetadata: PhoneMetadataOptions | None = Field(default=PhoneMetadataOptions.without)
    seiData: SEIDataOptions | None = Field(default=SEIDataOptions.any)
    minutesWithSeiData: Range | None = Field(default=None)
    fusedSeiData: SEIDataOptions | None = Field(default=SEIDataOptions.any)
    minutesWithFusedSei: Range | None = Field(default=None)
    safety_critical_events: SafetyCriticalEventsFilter | None = Field(default=None)
    ego_actions: EgoActionsFilter | None = Field(default=None)
    location: dict | None = Field(default=None)


class FootageLabels(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    weather: list[str] = Field(default_factory=list)
    temperature: Range | None = Field(default=None)
    timeOfDay: list[str] = Field(default_factory=list)
    roadType: list[str] = Field(default_factory=list)


class Users(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    user_ids: list[str] = Field(default_factory=list)
    camera_count: int | None = Field(default=None)
    vehicle_ids: list[str] = Field(default_factory=list)
    hw_versions: list[str] = Field(default_factory=list)
    car_versions: list[str] = Field(default_factory=list)
    device_ids: list[str] = Field(default_factory=list)


class FixedMetadataOutput(BaseModel):
    csv: TripCSVFormat | None = TripCSVFormat.default
    json_format: TripJSONFormat | None = TripJSONFormat.default
    mcap: TripMCAPFormat | None = None


class TripInsightOutput(BaseModel):
    base_insights: bool = True
    safety_critical_events: bool = False
    ego_actions: bool = False


class FootageOutput(BaseModel):
    embed_exif: bool = False
    csv: FootageCSVFormat | None = FootageCSVFormat.default
    mcap: FootageMCAPFormat | None = None
    sei: FootageSEIFormat | None = None
    fused_sei: FootageSEIFormat | None = None


class MinuteOutput(BaseModel):
    mapping_file: bool = False


class DatasetOutput(BaseModel):
    fixed_metadata: FixedMetadataOutput | None = Field(default_factory=FixedMetadataOutput)
    trip_insight: TripInsightOutput | None = Field(default_factory=TripInsightOutput)
    footage: FootageOutput = Field(default_factory=FootageOutput)
    minute: MinuteOutput = Field(default_factory=MinuteOutput)


class DatasetFilters(BaseModel):
    regions: list[Region] = Field(default_factory=list)
    trips: Trips | None = None
    labels: FootageLabels | None = None
    users: Users | None = None


class DatasetRequest(BaseModel):
    settings: DatasetSettings
    filters: DatasetFilters = Field(default_factory=DatasetFilters)
    output: DatasetOutput = Field(default_factory=DatasetOutput)


class SavedDatasetRequest(BaseModel):
    request_id: str | None = None
    request: DatasetRequest
