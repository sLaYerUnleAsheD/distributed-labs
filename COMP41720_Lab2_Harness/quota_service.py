from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn

app = FastAPI(title="Quota Service")

quotas = {"tenant_1": 100}
processed_transactions = set() # Idempotency store

class QuotaRequest(BaseModel):
    tenant_id: str
    correlation_id: str  # The Document ID

@app.post("/quota/reserve")
def reserve_quota(req: QuotaRequest):
    # IDEMPOTENCY CHECK
    if req.correlation_id in processed_transactions:
        return {"status": "already_reserved", "remaining": quotas[req.tenant_id]}

    if quotas.get(req.tenant_id, 0) > 0:
        quotas[req.tenant_id] -= 1
        processed_transactions.add(req.correlation_id)
        print(f"[Quota] Reserved for {req.correlation_id}. Remaining: {quotas[req.tenant_id]}")
        return {"status": "reserved", "remaining": quotas[req.tenant_id]}
    
    raise HTTPException(status_code=402, detail="Insufficient quota")

@app.post("/quota/release")
def release_quota(req: QuotaRequest):
    # IDEMPOTENCY CHECK - using a specific prefix for releases
    release_key = f"release_{req.correlation_id}"
    if release_key in processed_transactions:
        return {"status": "already_released", "remaining": quotas[req.tenant_id]}

    quotas[req.tenant_id] += 1
    processed_transactions.add(release_key)
    print(f"[Quota] Compensated (Released) for {req.correlation_id}. Remaining: {quotas[req.tenant_id]}")
    return {"status": "released", "remaining": quotas[req.tenant_id]}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8081)