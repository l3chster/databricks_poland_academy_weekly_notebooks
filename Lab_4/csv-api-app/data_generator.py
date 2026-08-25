import time
import fsspec
import asyncio
import csv
import json
from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from azure.identity import ClientSecretCredential
import os

storage_account = "dlspl21databricks"

container = os.environ["CONTAINER_NAME"]
tenant_id = os.environ["TENANT_ID"]
client_id = os.environ["CLIENT_ID"]
client_secret = os.environ["CLIENT_SECRET"]

source_path = f"abfs://{container}@{storage_account}.dfs.core.windows.net/raw_crime_incidents/crime_incidents_messy.csv"

credential = ClientSecretCredential(
    tenant_id=tenant_id,
    client_id=client_id,
    client_secret=client_secret
)

storage_options = {    
    "credential": credential
}

app = FastAPI()

async def generate_csv_stream():
    """Synthetic generator, which reads csv file row by row and streams it with delay."""
    with fsspec.open(source_path, mode="r", **storage_options) as file:
        reader = csv.reader(file)

        header = next(reader)
        
        for row in reader:
            await asyncio.sleep(1)             

            data = dict(zip(header, row))
            yield json.dumps(data) + "\n"

@app.get("/api/stream")
async def stream_data():
    """Endpoint which returns data as a continuous text stream (text/event-stream)."""
    return StreamingResponse(generate_csv_stream(), media_type="text/event-stream")