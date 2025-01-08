from fastapi import FastAPI, Request, Form, templating
from fastapi.responses import HTMLResponse,RedirectResponse
from app.models import TripUpdate
from app.utils import update_db
from dotenv import load_dotenv
from jinja2 import Environment, FileSystemLoader
import os

load_dotenv()

app = FastAPI()

env = Environment(loader=FileSystemLoader('templates'))
template = env.get_template('index.html')

@app.get("/", response_class=HTMLResponse)
async def read_root():
    return template.render()

@app.post("/update/")
async def update(request: Request,
                 ServiceId: int = Form(...),
                 DayofException: str = Form(""),
                 JourneyDate: str = Form(""),
                 boardingIds: str = Form(""),
                 droppingIds: str = Form(""),
                 timeChange: int = Form(...)):
    # Process form data
    data = {
        "ServiceId": ServiceId,
        "DayofException": [int(x) for x in DayofException.split(',') if x] if DayofException else [],
        "JourneyDate": JourneyDate.strip() if JourneyDate else None,
        "boardingIds": [int(x) for x in boardingIds.split(',') if x] if boardingIds else [],
        "droppingIds": [int(x) for x in droppingIds.split(',') if x] if droppingIds else [],
        "timeChange": timeChange
    }
    # Validate data against Pydantic model
    trip_update = TripUpdate(**data)
    # Update database
    update_db([trip_update.dict()])
    # Redirect to success page
    return RedirectResponse(url='/success/', status_code=303)

@app.get("/success/", response_class=HTMLResponse)
async def success():
    return "<h1>Update successful</h1>"

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)