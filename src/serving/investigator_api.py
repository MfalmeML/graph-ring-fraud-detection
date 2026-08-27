from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from typing import List, Optional
from neo4j import GraphDatabase
from src.community.investigator_interface import InvestigatorInterface
import os

app = FastAPI()

class RingResponse(BaseModel):
    ring_id: str
    risk_score: float
    size: int
    density: float
    detected_at: str
    member_ids: List[str]

class RingDetailResponse(BaseModel):
    ring_id: str
    risk_score: float
    size: int
    density: float
    detected_at: str
    status: str
    member_ids: List[str]
    shared_entities: List[str]

class UpdateStatusRequest(BaseModel):
    ring_id: str
    status: str
    investigator_id: str = "investigator"

class Config:
    def __init__(self):
        self.neo4j_uri = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
        self.neo4j_user = os.environ.get("NEO4J_USER", "neo4j")
        self.neo4j_password = os.environ.get("NEO4J_PASSWORD", "password")

config = Config()

def get_session():
    driver = GraphDatabase.driver(
        config.neo4j_uri,
        auth=(config.neo4j_user, config.neo4j_password)
    )
    try:
        with driver.session() as session:
            yield session
    finally:
        driver.close()

@app.get("/rings/pending", response_model=List[RingResponse])
def get_pending_rings(limit: int = 50, session=Depends(get_session)):
    interface = InvestigatorInterface(session)
    rings = interface.get_pending_rings(limit)
    return [
        RingResponse(
            ring_id=r["ring_id"],
            risk_score=r["risk_score"],
            size=r["size"],
            density=r["density"],
            detected_at=r["detected_at"],
            member_ids=r["member_ids"]
        )
        for r in rings
    ]

@app.get("/rings/{ring_id}", response_model=RingDetailResponse)
def get_ring_details(ring_id: str, session=Depends(get_session)):
    interface = InvestigatorInterface(session)
    ring = interface.get_ring_details(ring_id)
    if not ring:
        raise HTTPException(status_code=404, detail="Ring not found")
    return RingDetailResponse(
        ring_id=ring["ring_id"],
        risk_score=ring["risk_score"],
        size=ring["size"],
        density=ring["density"],
        detected_at=ring["detected_at"],
        status=ring["status"],
        member_ids=ring["member_ids"],
        shared_entities=ring["shared_entities"]
    )

@app.post("/rings/confirm")
def confirm_ring(request: UpdateStatusRequest, session=Depends(get_session)):
    interface = InvestigatorInterface(session)
    success = interface.confirm_ring(request.ring_id, request.investigator_id)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to confirm ring")
    return {"status": "confirmed", "ring_id": request.ring_id}

@app.post("/rings/reject")
def reject_ring(request: UpdateStatusRequest, session=Depends(get_session)):
    interface = InvestigatorInterface(session)
    success = interface.reject_ring(request.ring_id, request.investigator_id)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to reject ring")
    return {"status": "rejected", "ring_id": request.ring_id}

@app.get("/rings/labels/export")
def export_labels(session=Depends(get_session)):
    result = session.run(
        """
        MATCH (r:RingCandidate)
        WHERE r.status IN ['CONFIRMED', 'REJECTED']
        OPTIONAL MATCH (r)<-[:BELONGS_TO_RING]-(members:Account)
        RETURN r.id AS ring_id,
               r.status AS status,
               r.confirmed_at AS confirmed_at,
               r.confirmed_by AS confirmed_by,
               collect(members.id) AS member_ids
        """
    )
    labels = []
    for record in result:
        for member_id in record["member_ids"]:
            labels.append({
                "account_id": member_id,
                "ring_id": record["ring_id"],
                "label": 1 if record["status"] == "CONFIRMED" else 0,
                "confirmed_at": record["confirmed_at"],
                "confirmed_by": record["confirmed_by"]
            })
    return {"labels": labels}