import os, random, time, asyncio
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Generador de Respuestas")

FAILURE_RATE = float(os.getenv("FAILURE_RATE", "0.0"))

ZONES = {
    "providencia": {"buildings": 12500, "avg_area": 180.4, "density": "alta"},
    "las_condes":  {"buildings": 18700, "avg_area": 220.1, "density": "alta"},
    "maipu":       {"buildings": 31200, "avg_area": 95.3,  "density": "muy_alta"},
    "santiago":    {"buildings": 22100, "avg_area": 140.7, "density": "alta"},
    "nunoa":       {"buildings": 14300, "avg_area": 160.2, "density": "alta"},
    "pudahuel":    {"buildings": 19800, "avg_area": 88.5,  "density": "muy_alta"},
    "la_florida":  {"buildings": 25600, "avg_area": 102.3, "density": "alta"},
    "peñalolen":   {"buildings": 17400, "avg_area": 115.8, "density": "media"},
}


class QueryRequest(BaseModel):
    query_id:    str
    query_type:  str
    zone:        str
    retry_count: int = 0


@app.get("/health")
def health():
    return {"status": "ok", "failure_rate": FAILURE_RATE}


@app.post("/process")
async def process_query(req: QueryRequest):
    if random.random() < FAILURE_RATE:
        raise HTTPException(status_code=503, detail="Fallo temporal simulado")

    await asyncio.sleep(random.uniform(0.05, 0.2))

    zone_data = ZONES.get(req.zone, {"buildings": 5000, "avg_area": 120.0, "density": "media"})

    results = {
        "Q1": {"type": "count",     "value": zone_data["buildings"], "zone": req.zone},
        "Q2": {"type": "avg_area",  "value": zone_data["avg_area"],  "zone": req.zone},
        "Q3": {"type": "density",   "value": zone_data["density"],   "zone": req.zone},
        "Q4": {"type": "top_zones", "value": sorted(ZONES, key=lambda z: ZONES[z]["buildings"], reverse=True)[:3]},
        "Q5": {"type": "summary",   "value": zone_data},
    }

    return {
        "query_id":     req.query_id,
        "query_type":   req.query_type,
        "zone":         req.zone,
        "result":       results.get(req.query_type, results["Q1"]),
        "processed_at": time.time(),
    }


@app.post("/set_failure_rate")
def set_failure_rate(rate: float):
    global FAILURE_RATE
    FAILURE_RATE = max(0.0, min(1.0, rate))
    return {"failure_rate": FAILURE_RATE}
