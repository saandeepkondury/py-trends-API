import os
from typing import List, Optional, Dict, Any
from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from pytrends.request import TrendReq

API_KEY = os.getenv("API_KEY")  # set on the host

app = FastAPI(title="Pytrends API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

def require_key(x_api_key: Optional[str]):
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")

def _client():
    return TrendReq(
        hl="en-US", tz=0,
        timeout=(10, 25), retries=2, backoff_factor=0.1
    )

class IOTRequest(BaseModel):
    keywords: List[str] = Field(..., min_items=1, max_items=5)
    timeframe: str = "today 12-m"   # e.g., "now 7-d", "today 5-y"
    geo: str = ""                   # e.g., "US"
    gprop: str = ""                 # "", "news", "images", "youtube", "froogle"
    cat: int = 0                    # Google category id

class RelatedReq(IOTRequest):
    pass

@app.get("/health")
def health():
    return {"ok": True}

@app.post("/trends/interest_over_time")
def interest_over_time(req: IOTRequest, x_api_key: Optional[str] = Header(None)):
    require_key(x_api_key)
    py = _client()
    py.build_payload(req.keywords, cat=req.cat, timeframe=req.timeframe, geo=req.geo, gprop=req.gprop)
    df = py.interest_over_time()
    if df is None or df.empty:
        return {"data": [], "isPartial": False}
    # Convert to records; strip isPartial column but include summary
    is_partial_series = df["isPartial"] if "isPartial" in df.columns else None
    data = df.drop(columns=[c for c in ["isPartial"] if c in df.columns]).reset_index().to_dict(orient="records")
    return {
        "data": data,
        "meta": {
            "keywords": req.keywords,
            "timeframe": req.timeframe,
            "geo": req.geo,
            "gprop": req.gprop,
            "cat": req.cat,
            "isPartialAny": bool(is_partial_series.any()) if is_partial_series is not None else False
        }
    }

@app.post("/trends/related_queries")
def related_queries(req: RelatedReq, x_api_key: Optional[str] = Header(None)):
    require_key(x_api_key)
    py = _client()
    py.build_payload(req.keywords, cat=req.cat, timeframe=req.timeframe, geo=req.geo, gprop=req.gprop)
    rq: Dict[str, Dict[str, Any]] = py.related_queries()
    # Normalize pandas DataFrames to dict-of-records
    out = {}
    for kw, sections in (rq or {}).items():
        out[kw] = {}
        for name in ["top", "rising"]:
            df = sections.get(name)
            out[kw][name] = df.to_dict(orient="records") if df is not None else []
    return {"data": out, "meta": req.dict()}
