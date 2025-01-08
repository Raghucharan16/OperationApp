from pydantic import BaseModel, Field
from typing import Optional, List

class TripUpdate(BaseModel):
    ServiceId: int
    DayofException: Optional[List[int]] = None
    JourneyDate: Optional[str] = None
    boardingIds: Optional[List[int]] = None
    droppingIds: Optional[List[int]] = None
    timeChange: int