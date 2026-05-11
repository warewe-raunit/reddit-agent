"""
api.py - FastAPI entrypoint for the Reddit opportunity fetcher.

Run locally:
    uvicorn api:app --host 127.0.0.1 --port 8001

Then open:
    http://127.0.0.1:8001/docs   (Swagger UI)
    http://127.0.0.1:8001/redoc  (ReDoc, alternative layout)
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import replace
from typing import Any, Optional

import structlog
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from tools.opportunity_discovery_tool import DEFAULT_OPPORTUNITY_TYPES
from tools.opportunity_pipeline import (
    PipelineConfig,
    build_default_llm_review,
    build_pipeline_config_from_env,
    discover_opportunities_via_api,
)

load_dotenv(override=True)

logging.basicConfig(format="%(message)s", stream=sys.stdout, level=logging.INFO)
for _noisy in ("httpx", "httpcore", "openai", "langchain", "urllib3"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)

structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="%H:%M:%S", utc=False),
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
)

logger = structlog.get_logger(__name__)


def _split_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        parts = [part.strip() for part in value.replace("\n", ",").replace(";", ",").split(",")]
        return [part for part in parts if part]
    if isinstance(value, (list, tuple, set)):
        out: list[str] = []
        for item in value:
            out.extend(_split_list(item))
        seen: set[str] = set()
        deduped: list[str] = []
        for item in out:
            key = item.lower()
            if key not in seen:
                seen.add(key)
                deduped.append(item)
        return deduped
    text = str(value).strip()
    return [text] if text else []


def _csv(values: list[str]) -> str:
    return ", ".join(value for value in values if value)


class OpportunityFetcherTuning(BaseModel):
    """Optional knobs for scale, diagnostics, and rate-limit behavior."""

    model_config = ConfigDict(extra="forbid")

    queries_per_type: Optional[int] = Field(
        None,
        ge=1,
        le=50,
        description="How many generated search queries to run per selected bucket.",
    )
    pages_per_query: Optional[int] = Field(
        None,
        ge=1,
        le=10,
        description="How many Reddit search pages to read for each query.",
    )
    listing_limit: Optional[int] = Field(
        None,
        ge=1,
        le=100,
        description="Maximum Reddit listing children requested per search page.",
    )
    top_comments: Optional[int] = Field(
        None,
        ge=0,
        le=20,
        description="How many top comments to include in the LLM review payload.",
    )
    max_detail_fetches: Optional[int] = Field(
        None,
        ge=0,
        le=5000,
        description="0 means fetch all Stage 2 survivors; otherwise cap Stage 3 detail fetches.",
    )
    stage2_probe_max: Optional[int] = Field(
        None,
        ge=0,
        le=1000,
        description="Maximum vague-but-plausible titles rescued for body/comment probing.",
    )
    rejected_link_limit: Optional[int] = Field(
        None,
        ge=0,
        le=5000,
        description="How many rejected candidate links to include in coverage_report.rejected_candidates.",
    )
    session_files: list[str] = Field(
        default_factory=list,
        description="Optional saved Reddit session JSON files for authenticated search and parallel detail fetching.",
    )
    max_sessions: Optional[int] = Field(
        None,
        ge=0,
        le=100,
        description="Use up to this many discovered session files when session_files is empty.",
    )
    per_session_concurrency: Optional[int] = Field(
        None,
        ge=1,
        le=20,
        description="Parallel detail requests per Reddit session.",
    )
    global_detail_concurrency: Optional[int] = Field(
        None,
        ge=1,
        le=200,
        description="Global parallel detail request limit across all sessions.",
    )

    @field_validator("session_files", mode="before")
    @classmethod
    def _coerce_session_files(cls, value: Any) -> list[str]:
        return _split_list(value)


class ProductSection(BaseModel):
    """Section 1 — Product context. Who and what the product is."""

    model_config = ConfigDict(extra="forbid")

    product_name: str = Field(..., min_length=1, description="Public product name.")
    product_url: str = Field("", description="Product website URL. Used to skip already-mentioned threads.")
    product_description: str = Field(
        ...,
        min_length=20,
        description="Plain-English description of what the product does and who it helps.",
    )
    target_customer: str = Field(
        ...,
        min_length=2,
        description="Who the product is for, e.g. 'B2B sales teams' or 'Shopify store owners'.",
    )
    product_mention_terms: list[str] = Field(
        default_factory=list,
        description="Extra spellings/domains for the product so already-mentioned threads are skipped.",
    )

    @field_validator("product_mention_terms", mode="before")
    @classmethod
    def _coerce(cls, value: Any) -> list[str]:
        return _split_list(value)


class DiscoverySection(BaseModel):
    """Section 2 — Discovery signals. Drive the generated search queries."""

    model_config = ConfigDict(extra="forbid")

    pain_points: list[str] = Field(
        ...,
        min_length=1,
        description="Problems the product solves. These drive search queries and relevance filters.",
    )
    use_cases: list[str] = Field(
        default_factory=list,
        description="Concrete situations where the product is useful.",
    )
    keywords: list[str] = Field(
        ...,
        min_length=1,
        description="Search/relevance keywords for the product category and problem space.",
    )
    competitor_names: list[str] = Field(
        default_factory=list,
        description="Competitors or alternatives. Used to find comparison/alternative threads.",
    )
    search_queries: list[str] = Field(
        default_factory=list,
        description="Optional high-priority Reddit search queries to run before generated queries.",
    )

    @field_validator(
        "pain_points", "use_cases", "keywords", "competitor_names", "search_queries", mode="before"
    )
    @classmethod
    def _coerce(cls, value: Any) -> list[str]:
        return _split_list(value)


class FiltersSection(BaseModel):
    """Section 3 — Filters. Narrow what counts as a real opportunity."""

    model_config = ConfigDict(extra="forbid")

    required_context_terms: list[str] = Field(
        default_factory=list,
        description="Strict context terms. If set, a thread must contain at least one before LLM review.",
    )
    negative_keywords: list[str] = Field(
        default_factory=list,
        description="Avoid terms. Matching threads are rejected before LLM review.",
    )
    excluded_subreddits: list[str] = Field(
        default_factory=list,
        description="Subreddits to skip, without the r/ prefix.",
    )

    @field_validator(
        "required_context_terms", "negative_keywords", "excluded_subreddits", mode="before"
    )
    @classmethod
    def _coerce(cls, value: Any) -> list[str]:
        return _split_list(value)


class OutputSection(BaseModel):
    """Section 4 — Output shape. Controls result size and date windows."""

    model_config = ConfigDict(extra="forbid")

    opportunity_types: list[str] = Field(
        default_factory=lambda: list(DEFAULT_OPPORTUNITY_TYPES),
        description="Any of: recent, high_engagement, high_google_search. Empty means all.",
    )
    target_link_count: int = Field(20, ge=1, le=200, description="Maximum verified opportunities to return.")
    recent_days: int = Field(7, ge=1, le=3650, description="Posts this new are categorized as recent.")
    max_age_days: int = Field(730, ge=1, le=3650, description="Reject posts older than this many days.")
    use_llm: bool = Field(True, description="Use the configured LLM reviewer. False = heuristic fallback.")

    @field_validator("opportunity_types", mode="before")
    @classmethod
    def _coerce(cls, value: Any) -> list[str]:
        return _split_list(value)

    @model_validator(mode="after")
    def _validate_dates(self) -> "OutputSection":
        if self.recent_days > self.max_age_days:
            raise ValueError("recent_days cannot be greater than max_age_days")
        return self


class OpportunityFetchRequest(BaseModel):
    """
    Request body grouped into 5 sections:

    1. **product** — who/what the product is
    2. **discovery** — signals that drive search
    3. **filters** — context/negative/excluded
    4. **output** — result size and date windows
    5. **tuning** — optional scale and diagnostics knobs
    """

    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "examples": [
                {
                    "product": {
                        "product_name": "Acme CRM",
                        "product_url": "https://acme.example",
                        "product_description": "A lightweight CRM for small B2B sales teams.",
                        "target_customer": "founders, sales teams, agencies",
                        "product_mention_terms": ["acme", "acme.example"],
                    },
                    "discovery": {
                        "pain_points": ["messy follow-ups", "lost lead context", "manual CRM updates"],
                        "use_cases": ["tracking sales conversations", "automating follow-up reminders"],
                        "keywords": ["CRM", "sales pipeline", "lead tracking"],
                        "competitor_names": ["HubSpot", "Pipedrive"],
                        "search_queries": [
                            "\"sales pipeline\" \"follow up\"",
                            "\"Pipedrive\" alternative",
                        ],
                    },
                    "filters": {
                        "required_context_terms": ["CRM", "sales pipeline", "lead tracking"],
                        "negative_keywords": ["job application CRM", "game CRM"],
                        "excluded_subreddits": [],
                    },
                    "output": {
                        "opportunity_types": ["recent", "high_engagement", "high_google_search"],
                        "target_link_count": 20,
                        "recent_days": 7,
                        "max_age_days": 365,
                        "use_llm": True,
                    },
                    "tuning": {
                        "queries_per_type": 12,
                        "pages_per_query": 1,
                        "stage2_probe_max": 100,
                        "rejected_link_limit": 250,
                        "max_sessions": 3,
                    },
                }
            ]
        },
    )

    product: ProductSection = Field(..., description="Section 1 — Product context.")
    discovery: DiscoverySection = Field(..., description="Section 2 — Discovery signals.")
    filters: FiltersSection = Field(
        default_factory=FiltersSection, description="Section 3 — Filters."
    )
    output: OutputSection = Field(
        default_factory=OutputSection, description="Section 4 — Output shape."
    )
    tuning: OpportunityFetcherTuning = Field(
        default_factory=OpportunityFetcherTuning,
        description="Section 5 — Optional scale, sessions, and diagnostics tuning.",
    )

    @model_validator(mode="after")
    def _validate_context(self) -> "OpportunityFetchRequest":
        d = self.discovery
        if not (d.keywords or d.pain_points or d.use_cases):
            raise ValueError("Provide at least one keyword, pain point, or use case in discovery section")
        return self


def _apply_tuning(cfg: PipelineConfig, tuning: OpportunityFetcherTuning) -> PipelineConfig:
    updates: dict[str, Any] = {}
    for field_name in (
        "queries_per_type",
        "pages_per_query",
        "listing_limit",
        "top_comments",
        "max_detail_fetches",
        "stage2_probe_max",
        "rejected_link_limit",
        "max_sessions",
        "per_session_concurrency",
        "global_detail_concurrency",
    ):
        value = getattr(tuning, field_name)
        if value is not None:
            updates[field_name] = value
    if tuning.session_files:
        updates["session_files"] = tuple(tuning.session_files)
    return replace(cfg, **updates) if updates else cfg


API_DESCRIPTION = """
# Reddit Opportunity Fetcher API

Product-agnostic service that finds Reddit threads where a product can be **credibly and
helpfully mentioned**. The API is strictly **read-only**: it never posts, comments, votes,
DMs, or joins subreddits.

---

## How It Works

1. **You submit a product brief** (name, description, target customer, pain points, keywords,
   competitors, optional avoid terms, and discovery settings).
2. **The pipeline generates search queries** from the brief and runs them against Reddit search.
3. **Multi-stage filtering** scores candidates by recency, engagement, context match, and
   relevance — rejecting already-mentioned threads, off-topic posts, and negative-keyword hits.
4. **LLM review** (optional) re-ranks survivors and tags each opportunity with a reason.
5. **You get back** ranked opportunities grouped into buckets (`recent`, `high_engagement`,
   `high_google_search`) plus a detailed `coverage_report` for diagnostics.

---

## Quick Start

1. Hit `GET /health` to confirm the service is up and the LLM is configured.
2. Hit `GET /opportunity-fetcher/example-request` to get a ready-to-edit sample payload.
3. `POST /opportunity-fetcher/fetch` with your filled-in brief.

---

## Endpoint Sections

- **System** — health checks and service status.
- **Opportunity Fetcher** — example payload + the main discovery endpoint.

---

## Notes

- `recent_days` must be ≤ `max_age_days`.
- At least one of `keywords`, `pain_points`, or `use_cases` is required.
- `tuning` is optional — leave it empty for sensible defaults.
- Set `use_llm=false` to fall back to heuristic ranking (faster, cheaper, less accurate).
"""

OPENAPI_TAGS = [
    {
        "name": "System",
        "description": "Service health and configuration checks. Use these to verify the API is "
        "running and that an LLM key is loaded before sending real workloads.",
    },
    {
        "name": "Opportunity Fetcher",
        "description": "Product-agnostic Reddit opportunity discovery. Submit a product brief, "
        "get back ranked threads where the product can be helpfully mentioned. "
        "Start with the example-request endpoint, then POST to /fetch.",
    },
]

app = FastAPI(
    title="Reddit Opportunity Fetcher API",
    version="1.0.0",
    description=API_DESCRIPTION,
    openapi_tags=OPENAPI_TAGS,
    contact={"name": "Reddit Agent", "email": "manavsarna04@gmail.com"},
    docs_url="/docs",
    redoc_url="/redoc",
)


@app.get(
    "/health",
    tags=["System"],
    summary="Service health check",
    description=(
        "Returns `status: ok` when the API process is up. Also reports whether an "
        "LLM API key (`OPENROUTER_API_KEY` or `OPENAI_API_KEY`) is configured in the "
        "environment — needed for `use_llm=true` requests."
    ),
    response_description="Service status and LLM configuration flag.",
)
async def health() -> dict:
    return {
        "status": "ok",
        "service": "reddit-opportunity-fetcher",
        "llm_configured": bool(os.getenv("OPENROUTER_API_KEY") or os.getenv("OPENAI_API_KEY")),
    }


@app.get(
    "/opportunity-fetcher/example-request",
    tags=["Opportunity Fetcher"],
    summary="Get a ready-to-edit example request body",
    description=(
        "Returns the same example payload shown in the request schema. Copy it, edit the "
        "fields to describe your product, then POST it to `/opportunity-fetcher/fetch`."
    ),
    response_description="A sample JSON body matching the OpportunityFetchRequest schema.",
)
async def example_request() -> dict:
    return OpportunityFetchRequest.model_config["json_schema_extra"]["examples"][0]


@app.post(
    "/opportunity-fetcher/fetch",
    tags=["Opportunity Fetcher"],
    summary="Fetch Reddit product-promotion opportunities",
    description=(
        "**Main discovery endpoint.** Submit a product brief plus optional discovery and "
        "tuning settings; receive ranked Reddit threads where the product could be helpfully "
        "mentioned.\n\n"
        "**Inputs:**\n"
        "- Product context: `product_name`, `product_description`, `target_customer`, "
        "`product_url`, `product_mention_terms`.\n"
        "- Discovery signals: `pain_points`, `use_cases`, `keywords`, `competitor_names`, "
        "`search_queries`.\n"
        "- Filters: `required_context_terms`, `negative_keywords`, `excluded_subreddits`, "
        "`max_age_days`, `recent_days`.\n"
        "- Output shape: `opportunity_types`, `target_link_count`.\n"
        "- Scaling knobs: `tuning` (sessions, concurrency, page depth, diagnostics caps).\n\n"
        "**Response:** the existing opportunity JSON contract — ranked buckets plus a "
        "`coverage_report` describing what was searched, what was rejected, and why."
    ),
    response_description="Ranked opportunity buckets and coverage_report diagnostics.",
)
async def fetch_opportunities(request: OpportunityFetchRequest) -> dict:
    p, d, f, o = request.product, request.discovery, request.filters, request.output

    cfg = build_pipeline_config_from_env(
        target_link_count=o.target_link_count,
        max_age_days=o.max_age_days,
        recent_days=o.recent_days,
    )
    cfg = _apply_tuning(cfg, request.tuning)
    llm_review = build_default_llm_review() if o.use_llm else None

    try:
        result = await discover_opportunities_via_api(
            product_name=p.product_name,
            product_url=p.product_url,
            product_description=p.product_description,
            target_customer=p.target_customer,
            pain_points=_csv(d.pain_points),
            use_cases=_csv(d.use_cases),
            keywords=_csv(d.keywords),
            competitor_names=_csv(d.competitor_names),
            excluded_subreddits=_csv(f.excluded_subreddits),
            product_mention_terms=_csv(p.product_mention_terms),
            search_queries=_csv(d.search_queries),
            required_context_terms=_csv(f.required_context_terms),
            negative_keywords=_csv(f.negative_keywords),
            opportunity_types=_csv(o.opportunity_types),
            target_link_count=o.target_link_count,
            max_age_days=o.max_age_days,
            recent_days=o.recent_days,
            llm_review=llm_review,
            config=cfg,
        )
    except Exception as exc:
        logger.exception("opportunity_fetcher_api_failed", error=str(exc))
        raise HTTPException(status_code=500, detail=f"Opportunity fetch failed: {exc}") from exc

    coverage = result.setdefault("coverage_report", {})
    coverage["api_request_summary"] = {
        "product_name": p.product_name,
        "selected_opportunity_types": o.opportunity_types,
        "target_link_count": o.target_link_count,
        "recent_days": o.recent_days,
        "max_age_days": o.max_age_days,
        "llm_requested": o.use_llm,
        "llm_configured": llm_review is not None,
        "custom_search_queries": len(d.search_queries),
        "negative_keywords": f.negative_keywords,
        "required_context_terms": f.required_context_terms,
    }
    return result


FORM_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Reddit Opportunity Fetcher</title>
<style>
  :root { --bg:#0f1115; --panel:#171a21; --border:#2a2f3a; --text:#e6e8ee; --muted:#9aa3b2; --accent:#ff4500; }
  * { box-sizing: border-box; }
  body { margin:0; font-family: ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, sans-serif;
         background:var(--bg); color:var(--text); line-height:1.45; }
  header { padding:24px 32px; border-bottom:1px solid var(--border); background:#12151c; }
  header h1 { margin:0 0 4px; font-size:22px; }
  header p { margin:0; color:var(--muted); font-size:14px; }
  main { max-width:980px; margin:0 auto; padding:24px 16px 80px; }
  fieldset { background:var(--panel); border:1px solid var(--border); border-radius:10px;
             padding:18px 20px; margin:0 0 18px; }
  legend { padding:0 8px; font-weight:600; color:var(--accent); }
  .hint { color:var(--muted); font-size:13px; margin:-4px 0 14px; }
  .grid { display:grid; grid-template-columns:repeat(2, minmax(0,1fr)); gap:14px 18px; }
  .grid.three { grid-template-columns:repeat(3, minmax(0,1fr)); }
  .field { display:flex; flex-direction:column; gap:6px; }
  .field.full { grid-column:1/-1; }
  label { font-size:13px; color:var(--muted); }
  label .req { color:var(--accent); margin-left:4px; }
  input[type=text], input[type=number], textarea, select {
    background:#0c0e13; color:var(--text); border:1px solid var(--border);
    border-radius:8px; padding:9px 10px; font:inherit; outline:none; width:100%;
  }
  textarea { min-height:70px; resize:vertical; font-family:inherit; }
  input:focus, textarea:focus, select:focus { border-color:var(--accent); }
  .row-check { display:flex; align-items:center; gap:8px; color:var(--muted); font-size:13px; }
  .actions { display:flex; gap:12px; align-items:center; margin-top:6px; }
  button { background:var(--accent); color:#fff; border:0; border-radius:8px;
           padding:11px 22px; font-weight:600; cursor:pointer; font-size:14px; }
  button.secondary { background:#2a2f3a; }
  button:disabled { opacity:0.6; cursor:wait; }
  pre#out { background:#0c0e13; border:1px solid var(--border); border-radius:10px;
            padding:14px; max-height:520px; overflow:auto; font-size:12.5px; white-space:pre-wrap; }
  .status { color:var(--muted); font-size:13px; }
  a { color:var(--accent); }
</style>
</head>
<body>
<header>
  <h1>Reddit Opportunity Fetcher</h1>
  <p>Fill the sections below and submit. Read-only: never posts, comments, or votes.
     Raw API at <a href="/docs">/docs</a>.</p>
</header>
<main>
<form id="f">

  <fieldset>
    <legend>1. Product</legend>
    <p class="hint">Who and what the product is. These drive every relevance check.</p>
    <div class="grid">
      <div class="field"><label>Product name<span class="req">*</span></label>
        <input name="product.product_name" required placeholder="Acme CRM"></div>
      <div class="field"><label>Product URL</label>
        <input name="product.product_url" placeholder="https://acme.example"></div>
      <div class="field full"><label>Product description<span class="req">*</span> (min 20 chars)</label>
        <textarea name="product.product_description" required minlength="20"
          placeholder="A lightweight CRM for small B2B sales teams."></textarea></div>
      <div class="field full"><label>Target customer<span class="req">*</span></label>
        <input name="product.target_customer" required placeholder="founders, sales teams, agencies"></div>
      <div class="field full"><label>Product mention terms (comma separated)</label>
        <input name="product.product_mention_terms" placeholder="acme, acme.example, acmecrm"></div>
    </div>
  </fieldset>

  <fieldset>
    <legend>2. Discovery Signals</legend>
    <p class="hint">Comma-separated lists. These build the search queries.</p>
    <div class="grid">
      <div class="field full"><label>Pain points<span class="req">*</span></label>
        <input name="discovery.pain_points" required placeholder="messy follow-ups, lost lead context"></div>
      <div class="field full"><label>Use cases</label>
        <input name="discovery.use_cases" placeholder="tracking sales conversations, automating reminders"></div>
      <div class="field full"><label>Keywords<span class="req">*</span></label>
        <input name="discovery.keywords" required placeholder="CRM, sales pipeline, lead tracking"></div>
      <div class="field full"><label>Competitor names</label>
        <input name="discovery.competitor_names" placeholder="HubSpot, Pipedrive"></div>
      <div class="field full"><label>Extra search queries (one per line or comma)</label>
        <textarea name="discovery.search_queries" placeholder='"sales pipeline" "follow up"&#10;"Pipedrive" alternative'></textarea></div>
    </div>
  </fieldset>

  <fieldset>
    <legend>3. Filters</legend>
    <p class="hint">Narrow what counts as a real opportunity.</p>
    <div class="grid">
      <div class="field full"><label>Required context terms</label>
        <input name="filters.required_context_terms" placeholder="CRM, sales pipeline"></div>
      <div class="field full"><label>Negative keywords (reject if present)</label>
        <input name="filters.negative_keywords" placeholder="job application CRM, game CRM"></div>
      <div class="field full"><label>Excluded subreddits (no r/ prefix)</label>
        <input name="filters.excluded_subreddits" placeholder="memes, funny"></div>
    </div>
  </fieldset>

  <fieldset>
    <legend>4. Output Shape</legend>
    <div class="grid three">
      <div class="field"><label>Opportunity types</label>
        <input name="output.opportunity_types" placeholder="recent, high_engagement, high_google_search"></div>
      <div class="field"><label>Target link count (1-200)</label>
        <input type="number" name="output.target_link_count" value="20" min="1" max="200"></div>
      <div class="field"><label>Recent days</label>
        <input type="number" name="output.recent_days" value="7" min="1" max="3650"></div>
      <div class="field"><label>Max age days</label>
        <input type="number" name="output.max_age_days" value="365" min="1" max="3650"></div>
      <div class="field"><label>&nbsp;</label>
        <label class="row-check"><input type="checkbox" name="output.use_llm" checked> Use LLM reviewer</label></div>
    </div>
  </fieldset>

  <fieldset>
    <legend>5. Tuning (optional)</legend>
    <p class="hint">Leave blank for defaults. Controls scale, sessions, diagnostics.</p>
    <div class="grid three">
      <div class="field"><label>queries_per_type</label><input type="number" name="tuning.queries_per_type" min="1" max="50"></div>
      <div class="field"><label>pages_per_query</label><input type="number" name="tuning.pages_per_query" min="1" max="10"></div>
      <div class="field"><label>listing_limit</label><input type="number" name="tuning.listing_limit" min="1" max="100"></div>
      <div class="field"><label>top_comments</label><input type="number" name="tuning.top_comments" min="0" max="20"></div>
      <div class="field"><label>max_detail_fetches</label><input type="number" name="tuning.max_detail_fetches" min="0" max="5000"></div>
      <div class="field"><label>stage2_probe_max</label><input type="number" name="tuning.stage2_probe_max" min="0" max="1000"></div>
      <div class="field"><label>rejected_link_limit</label><input type="number" name="tuning.rejected_link_limit" min="0" max="5000"></div>
      <div class="field"><label>max_sessions</label><input type="number" name="tuning.max_sessions" min="0" max="100"></div>
      <div class="field"><label>per_session_concurrency</label><input type="number" name="tuning.per_session_concurrency" min="1" max="20"></div>
      <div class="field"><label>global_detail_concurrency</label><input type="number" name="tuning.global_detail_concurrency" min="1" max="200"></div>
      <div class="field full"><label>Session files (comma separated paths)</label>
        <input name="tuning.session_files" placeholder="sessions/a.json, sessions/b.json"></div>
    </div>
  </fieldset>

  <div class="actions">
    <button type="submit" id="go">Fetch opportunities</button>
    <button type="button" class="secondary" id="loadExample">Load example</button>
    <span class="status" id="status"></span>
  </div>
</form>

<h3 style="margin-top:28px;">Response</h3>
<pre id="out">Submit the form to see the JSON response here.</pre>

<script>
const LIST_FIELDS = new Set([
  "product.product_mention_terms",
  "discovery.pain_points","discovery.use_cases","discovery.keywords",
  "discovery.competitor_names","discovery.search_queries",
  "filters.required_context_terms","filters.negative_keywords","filters.excluded_subreddits",
  "output.opportunity_types",
  "tuning.session_files",
]);
const INT_FIELDS = new Set([
  "output.target_link_count","output.recent_days","output.max_age_days",
  "tuning.queries_per_type","tuning.pages_per_query","tuning.listing_limit",
  "tuning.top_comments","tuning.max_detail_fetches","tuning.stage2_probe_max",
  "tuning.rejected_link_limit","tuning.max_sessions",
  "tuning.per_session_concurrency","tuning.global_detail_concurrency",
]);

function splitList(v) {
  return v.split(/[,\n;]/).map(s => s.trim()).filter(Boolean);
}

function buildPayload(form) {
  const body = { product: {}, discovery: {}, filters: {}, output: {}, tuning: {} };
  for (const el of form.elements) {
    if (!el.name) continue;
    const dot = el.name.indexOf(".");
    if (dot < 0) continue;
    const section = el.name.slice(0, dot);
    const key = el.name.slice(dot + 1);
    let v;
    if (el.type === "checkbox") { body[section][key] = el.checked; continue; }
    v = el.value.trim();
    if (v === "") continue;
    if (LIST_FIELDS.has(el.name)) v = splitList(v);
    else if (INT_FIELDS.has(el.name)) v = Number(v);
    body[section][key] = v;
  }
  for (const k of ["filters","output","tuning"]) {
    if (Object.keys(body[k]).length === 0) delete body[k];
  }
  return body;
}

document.getElementById("loadExample").onclick = async () => {
  const r = await fetch("/opportunity-fetcher/example-request");
  const ex = await r.json();
  const form = document.getElementById("f");
  const setVal = (name, v) => {
    const el = form.elements[name];
    if (!el) return;
    if (Array.isArray(v)) el.value = v.join(", ");
    else if (typeof v === "boolean") el.checked = v;
    else el.value = v ?? "";
  };
  for (const [section, fields] of Object.entries(ex)) {
    if (fields && typeof fields === "object" && !Array.isArray(fields)) {
      for (const [k, v] of Object.entries(fields)) setVal(section + "." + k, v);
    }
  }
};

document.getElementById("f").onsubmit = async (e) => {
  e.preventDefault();
  const btn = document.getElementById("go");
  const status = document.getElementById("status");
  const out = document.getElementById("out");
  const payload = buildPayload(e.target);
  btn.disabled = true; status.textContent = "Running...";
  out.textContent = "Fetching... (this can take a while)";
  const t0 = performance.now();
  try {
    const r = await fetch("/opportunity-fetcher/fetch", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    const text = await r.text();
    let pretty = text;
    try { pretty = JSON.stringify(JSON.parse(text), null, 2); } catch {}
    out.textContent = pretty;
    status.textContent = `HTTP ${r.status} in ${((performance.now()-t0)/1000).toFixed(1)}s`;
  } catch (err) {
    out.textContent = String(err);
    status.textContent = "Error";
  } finally {
    btn.disabled = false;
  }
};
</script>
</main>
</body>
</html>
"""


@app.get("/", include_in_schema=False)
async def root() -> RedirectResponse:
    return RedirectResponse(url="/form")


@app.get(
    "/form",
    tags=["Opportunity Fetcher"],
    summary="HTML form UI with sectioned inputs",
    description="Browser form with sections for Product, Discovery, Filters, Output, Tuning. "
    "Submits to /opportunity-fetcher/fetch and renders the JSON response inline.",
    response_class=HTMLResponse,
)
async def form_page() -> HTMLResponse:
    return HTMLResponse(content=FORM_HTML)
