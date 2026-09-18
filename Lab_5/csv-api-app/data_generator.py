import time
import asyncio
import csv
import json
import io
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from databricks.sdk import WorkspaceClient
import os

catalog_path = os.environ["SOURCE_CATALOG"]
source_path = f"{catalog_path}/crime_incidents_messy.csv"

app = FastAPI()
w = WorkspaceClient()

async def generate_csv_stream():
    """Synthetic generator, which reads csv file row by row and streams it with delay."""
    
    response = w.files.download(source_path)
    raw_bytes = response.contents.read()
    text = raw_bytes.decode("utf-8")

    reader = csv.reader(io.StringIO(text))
    header = next(reader)
        
    for row in reader:
        await asyncio.sleep(1)             

        data = dict(zip(header, row))
        yield json.dumps(data) + "\n"

@app.get("/api/stream")
async def stream_data():
    """Endpoint which returns data as a continuous text stream (text/event-stream)."""
    return StreamingResponse(generate_csv_stream(), media_type="text/event-stream")