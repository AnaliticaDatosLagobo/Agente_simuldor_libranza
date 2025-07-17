from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from .core import simular_libranza

app = FastAPI(
    title="Simulador de Libranza",
    description="API para calcular simulaciones financieras de libranzas",
    version="1.0.0"
)

class SimulacionInput(BaseModel):
    nro_libranza: int

@app.get("/")
def root():
    return {"message": "Bienvenido a la API de simulación de libranza"}

@app.post("/simular/")
def simular(input: SimulacionInput):
    try:
        resultado = simular_libranza(input.nro_libranza)
        return resultado
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))