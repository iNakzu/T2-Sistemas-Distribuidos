from fastapi import FastAPI, HTTPException
import pandas as pd
from queries import q1_count, q2_area, q3_density, q4_compare, q5_confidence_dist

app = FastAPI()

DATA_PATH = "/app/data/buildings.csv"

data_by_zone = {}
zone_area_km2 = {
    "Z1": 9.2,
    "Z2": 15.0,
    "Z3": 18.5,
    "Z4": 10.8,
    "Z5": 16.4
}

@app.on_event("startup")
def startup_event():
    global data_by_zone
    df = pd.read_csv(DATA_PATH)

    data_by_zone = {}
    for zone_id, group in df.groupby("zone_id"):
        data_by_zone[zone_id] = group.to_dict(orient="records")

    print("Dataset cargado en memoria correctamente.")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/query")
def query(payload: dict):
    query_type = payload.get("type")
    params = payload.get("params", {})

    try:
        if query_type == "Q1":
            result = q1_count(data_by_zone, params["zone_id"], params.get("confidence_min", 0.0))

        elif query_type == "Q2":
            result = q2_area(data_by_zone, params["zone_id"], params.get("confidence_min", 0.0))

        elif query_type == "Q3":
            result = q3_density(data_by_zone, zone_area_km2, params["zone_id"], params.get("confidence_min", 0.0))

        elif query_type == "Q4":
            result = q4_compare(
                data_by_zone,
                zone_area_km2,
                params["zone_a"],
                params["zone_b"],
                params.get("confidence_min", 0.0)
            )

        elif query_type == "Q5":
            result = q5_confidence_dist(data_by_zone, params["zone_id"], params.get("bins", 5))

        else:
            raise HTTPException(status_code=400, detail="Tipo de consulta no válido")

        return {"result": result}

    except KeyError as e:
        raise HTTPException(status_code=400, detail=f"Falta parámetro: {str(e)}")
