"""
Seismic MCP Server — seismic data & insurance/parametric analytics.

Transports:
  * streamable-http (default)  -> POST/GET/DELETE http://<host>:<port>/mcp
  * stdio                      -> MCP_TRANSPORT=stdio (Claude Desktop local)

Logs go to stderr only.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import math
import os
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Any

import httpx
import jsonschema
import mcp.types as types
from mcp.server.lowlevel import Server
from mcp.shared.exceptions import McpError

# ---------------------------------------------------------------------------
# Config / logging
# ---------------------------------------------------------------------------

SERVER_NAME = "seismic-mcp-server"
SERVER_VERSION = "0.1.0"

USGS_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
TMD_URL = "https://data.tmd.go.th/api/DailySeismicEvent/v1/"
TMD_DEMO_PARAMS = {"uid": "api", "ukey": "api12345", "format": "json"}
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "30"))

logging.basicConfig(
    stream=sys.stderr,
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger(SERVER_NAME)

# JSON-RPC error codes
INVALID_PARAMS = -32602
INTERNAL_ERROR = -32603
UPSTREAM_ERROR = -32000  # server-defined: upstream HTTP / parse failure


def rpc_error(code: int, message: str) -> McpError:
    return McpError(types.ErrorData(code=code, message=message))


# ---------------------------------------------------------------------------
# Tool schemas (from spec)
# ---------------------------------------------------------------------------

ASSET_PROPS = {
    "asset_id": {"type": "string"},
    "name": {"type": "string"},
    "latitude": {"type": "number"},
    "longitude": {"type": "number"},
}

TOOLS: list[dict[str, Any]] = [
    {
        "name": "usgs_search_earthquakes",
        "description": "Search earthquakes from the USGS FDSN API with basic filters.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "starttime": {"type": "string", "description": "YYYY-MM-DD or ISO8601."},
                "endtime": {"type": "string", "description": "YYYY-MM-DD or ISO8601."},
                "minmagnitude": {"type": "number", "description": "Minimum magnitude.", "default": 4.5},
                "maxmagnitude": {"type": "number", "description": "Maximum magnitude, optional."},
                "latitude": {"type": "number", "description": "Center latitude for radius search, optional."},
                "longitude": {"type": "number", "description": "Center longitude for radius search, optional."},
                "maxradiuskm": {"type": "number", "description": "Search radius in kilometers, optional."},
                "limit": {
                    "type": "integer",
                    "description": "Maximum number of events.",
                    "default": 20,
                    "minimum": 1,
                    "maximum": 200,
                },
            },
            "required": ["starttime", "endtime"],
            "additionalProperties": False,
        },
    },
    {
        "name": "tmd_get_daily_earthquakes",
        "description": "Get daily earthquakes from the TMD DailySeismicEvent demo API.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "date": {"type": "string", "description": "YYYY-MM-DD. If omitted, use today's date."},
            },
            "required": [],
            "additionalProperties": False,
        },
    },
    {
        "name": "tmd_get_recent_earthquakes",
        "description": "Get recent earthquakes from TMD over the last N days using DailySeismicEvent.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "days": {
                    "type": "integer",
                    "description": "Number of days back from today (including today).",
                    "default": 3,
                    "minimum": 1,
                    "maximum": 7,
                },
            },
            "required": [],
            "additionalProperties": False,
        },
    },
    {
        "name": "match_events_to_assets",
        "description": "Match earthquake events to insured assets within a given radius and magnitude threshold.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "events": {
                    "type": "array",
                    "description": "List of EarthquakeEvent objects.",
                    "items": {"type": "object"},
                },
                "assets": {
                    "type": "array",
                    "description": "Insured assets with coordinates.",
                    "items": {
                        "type": "object",
                        "properties": {
                            **ASSET_PROPS,
                            "sum_insured": {"type": "number", "description": "Optional total insured value."},
                        },
                        "required": ["asset_id", "latitude", "longitude"],
                        "additionalProperties": True,
                    },
                },
                "max_distance_km": {
                    "type": "number",
                    "description": "Max distance to consider an asset affected.",
                    "default": 100,
                },
                "min_magnitude": {"type": "number", "description": "Minimum magnitude to consider.", "default": 4.5},
            },
            "required": ["events", "assets"],
            "additionalProperties": False,
        },
    },
    {
        "name": "compute_parametric_payout",
        "description": (
            "Compute a simple parametric payout for an earthquake event and a set of assets, "
            "based on distance and magnitude thresholds."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "event": {"type": "object", "description": "Single EarthquakeEvent to evaluate."},
                "assets": {
                    "type": "array",
                    "description": "Assets with coordinates and sum_insured.",
                    "items": {
                        "type": "object",
                        "properties": {**ASSET_PROPS, "sum_insured": {"type": "number"}},
                        "required": ["asset_id", "latitude", "longitude", "sum_insured"],
                        "additionalProperties": True,
                    },
                },
                "min_magnitude_trigger": {
                    "type": "number",
                    "description": "Minimum magnitude to trigger any payout.",
                    "default": 5.0,
                },
                "max_distance_km_trigger": {
                    "type": "number",
                    "description": "Max distance for eligibility.",
                    "default": 200,
                },
                "payout_curve": {
                    "type": "array",
                    "description": "Optional distance-based payout curve.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "max_distance_km": {"type": "number"},
                            "payout_percent": {
                                "type": "number",
                                "description": "Fraction of sum_insured (0.0–1.0).",
                            },
                        },
                        "required": ["max_distance_km", "payout_percent"],
                        "additionalProperties": False,
                    },
                },
            },
            "required": ["event", "assets"],
            "additionalProperties": False,
        },
    },
]
TOOLS_BY_NAME = {t["name"]: t for t in TOOLS}

DEFAULT_PAYOUT_CURVE = [
    {"max_distance_km": 50, "payout_percent": 0.5},
    {"max_distance_km": 100, "payout_percent": 0.25},
    {"max_distance_km": 200, "payout_percent": 0.1},
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def apply_defaults(schema: dict, args: dict) -> dict:
    out = dict(args)
    for key, prop in schema.get("properties", {}).items():
        if key not in out and "default" in prop:
            out[key] = prop["default"]
    return out


def validate_args(tool_name: str, args: dict | None) -> dict:
    tool = TOOLS_BY_NAME.get(tool_name)
    if tool is None:
        raise rpc_error(INVALID_PARAMS, f"Unknown tool: {tool_name}")
    args = args or {}
    try:
        jsonschema.validate(args, tool["inputSchema"])
    except jsonschema.ValidationError as e:
        path = ".".join(str(p) for p in e.absolute_path) or "<root>"
        raise rpc_error(INVALID_PARAMS, f"Invalid arguments at {path}: {e.message}") from None
    return apply_defaults(tool["inputSchema"], args)


def to_float(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    r = 6371.0088
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def parse_date(s: str, field: str) -> date:
    try:
        return date.fromisoformat(s[:10])
    except ValueError:
        raise rpc_error(INVALID_PARAMS, f"'{field}' must be YYYY-MM-DD, got {s!r}") from None


def today_local() -> date:
    return datetime.now().astimezone().date()


def require_event_coords(ev: dict, label: str) -> tuple[float, float, float | None]:
    lat, lon = to_float(ev.get("latitude")), to_float(ev.get("longitude"))
    if lat is None or lon is None:
        raise rpc_error(INVALID_PARAMS, f"{label} is missing numeric latitude/longitude")
    return lat, lon, to_float(ev.get("magnitude"))


async def http_get_json(client: httpx.AsyncClient, url: str, params: dict) -> Any:
    try:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
    except httpx.HTTPStatusError as e:
        raise rpc_error(UPSTREAM_ERROR, f"HTTP {e.response.status_code} from {url}: {e.response.text[:300]}") from None
    except httpx.HTTPError as e:
        raise rpc_error(UPSTREAM_ERROR, f"HTTP request to {url} failed: {e!r}") from None
    try:
        return resp.json()
    except ValueError:
        raise rpc_error(UPSTREAM_ERROR, f"Could not parse JSON from {url}") from None


def new_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        timeout=HTTP_TIMEOUT,
        follow_redirects=True,
        headers={"User-Agent": f"{SERVER_NAME}/{SERVER_VERSION}"},
    )


# ---------------------------------------------------------------------------
# Normalizers -> EarthquakeEvent
# ---------------------------------------------------------------------------


def usgs_to_event(feature: dict) -> dict:
    props = feature.get("properties") or {}
    coords = (feature.get("geometry") or {}).get("coordinates") or [None, None, None]
    coords = list(coords) + [None] * (3 - len(coords))
    t = props.get("time")
    iso = datetime.fromtimestamp(t / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z") if t is not None else None
    return {
        "id": feature.get("id"),
        "time": iso,
        "place": props.get("place"),
        "magnitude": props.get("mag"),
        "latitude": coords[1],
        "longitude": coords[0],
        "depth_km": coords[2],
        "source": "USGS",
        "url": props.get("url"),
        "raw": feature,
    }


def _tmd_time(item: dict) -> str | None:
    """Return ISO8601 time. Supports spec shape (DateTime) and live shape (DateTimeUTC)."""
    if item.get("DateTime"):
        return str(item["DateTime"])
    if item.get("DateTimeUTC"):
        try:
            dt = datetime.fromisoformat(str(item["DateTimeUTC"]).replace(" ", "T"))
            return dt.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
        except ValueError:
            return str(item["DateTimeUTC"])
    if item.get("DateTimeThai"):
        try:
            dt = datetime.fromisoformat(str(item["DateTimeThai"]).replace(" ", "T"))
            return dt.replace(tzinfo=timezone(timedelta(hours=7))).isoformat()
        except ValueError:
            return str(item["DateTimeThai"])
    return None


def _tmd_local_date(item: dict) -> str | None:
    """Thai-local calendar date (YYYY-MM-DD) of the event, used for daily filtering."""
    if item.get("DateTimeThai"):
        return str(item["DateTimeThai"])[:10]
    t = _tmd_time(item)
    if not t:
        return None
    try:
        dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
        return dt.astimezone(timezone(timedelta(hours=7))).date().isoformat()
    except ValueError:
        return t[:10]


def tmd_to_event(item: dict) -> dict:
    lat = to_float(item.get("Latitude"))
    lon = to_float(item.get("Longitude"))
    t = _tmd_time(item)
    if item.get("EventID"):
        eid = f"TMD-{item['EventID']}"
    else:
        # Live API has no EventID: derive a stable id from time + epicenter.
        key = f"{item.get('DateTimeUTC') or t}|{lat}|{lon}"
        eid = "TMD-" + hashlib.sha1(key.encode()).hexdigest()[:12]
    place = item.get("Location") or item.get("OriginThai") or item.get("TitleThai")
    return {
        "id": eid,
        "time": t,
        "place": place,
        "magnitude": to_float(item.get("Magnitude")),
        "latitude": lat,
        "longitude": lon,
        "depth_km": to_float(item.get("DepthKm", item.get("Depth"))),
        "source": "TMD_DailySeismicEvent",
        "url": item.get("URL") or item.get("Url") or item.get("url"),
        "raw": item,
    }


def tmd_items(payload: Any) -> list[dict]:
    if not isinstance(payload, dict):
        raise rpc_error(UPSTREAM_ERROR, "Unexpected TMD response (not a JSON object)")
    items = payload.get("DailySeismicEvent")
    if items is None:
        items = payload.get("DailyEarthquakes")
    if items is None:
        return []
    if isinstance(items, dict):  # single item serialized as object
        items = [items]
    return [i for i in items if isinstance(i, dict)]


async def tmd_fetch_day(client: httpx.AsyncClient, day: str) -> list[dict]:
    payload = await http_get_json(client, TMD_URL, {**TMD_DEMO_PARAMS, "Date": day})
    items = tmd_items(payload)
    # The live demo endpoint may ignore `Date` and return a long history,
    # so keep only items whose Thai-local date matches the requested day.
    return [i for i in items if _tmd_local_date(i) == day]


def sort_key_time(ev: dict) -> datetime:
    t = ev.get("time")
    if not t:
        return datetime.min.replace(tzinfo=timezone.utc)
    try:
        dt = datetime.fromisoformat(str(t).replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Tool implementations
# ---------------------------------------------------------------------------


async def usgs_search_earthquakes(args: dict) -> dict:
    geo = [k for k in ("latitude", "longitude", "maxradiuskm") if k in args]
    if geo and len(geo) != 3:
        raise rpc_error(INVALID_PARAMS, "latitude, longitude and maxradiuskm must be provided together")
    query = {"format": "geojson", **args}
    async with new_client() as client:
        data = await http_get_json(client, USGS_URL, query)
    if not isinstance(data, dict) or not isinstance(data.get("features"), list):
        raise rpc_error(UPSTREAM_ERROR, "Unexpected USGS response (no features array)")
    return {"events": [usgs_to_event(f) for f in data["features"]], "query": query}


async def tmd_get_daily_earthquakes(args: dict) -> dict:
    day = parse_date(args["date"], "date").isoformat() if args.get("date") else today_local().isoformat()
    async with new_client() as client:
        items = await tmd_fetch_day(client, day)
    events = sorted((tmd_to_event(i) for i in items), key=sort_key_time, reverse=True)
    return {"events": events, "query": {"date": day}}


async def tmd_get_recent_earthquakes(args: dict) -> dict:
    days = args["days"]
    today = today_local()
    dates = [(today - timedelta(days=d)).isoformat() for d in range(days)]
    async with new_client() as client:
        results = await asyncio.gather(*(tmd_fetch_day(client, d) for d in dates))
    seen: dict[str, dict] = {}
    for items in results:
        for item in items:
            ev = tmd_to_event(item)
            seen.setdefault(ev["id"], ev)
    events = sorted(seen.values(), key=sort_key_time, reverse=True)
    return {"events": events, "query": {"days": days}}


async def match_events_to_assets(args: dict) -> dict:
    max_d = float(args["max_distance_km"])
    min_m = float(args["min_magnitude"])
    events = []
    for idx, ev in enumerate(args["events"]):
        lat, lon, mag = require_event_coords(ev, f"events[{idx}]")
        if mag is not None and mag >= min_m:
            events.append((ev, lat, lon))

    matches = []
    for asset in args["assets"]:
        hits = []
        for ev, lat, lon in events:
            d = haversine_km(lat, lon, float(asset["latitude"]), float(asset["longitude"]))
            if d <= max_d:
                hits.append({"event": ev, "distance_km": round(d, 2)})
        if hits:
            hits.sort(key=lambda h: h["distance_km"])
            matches.append(
                {
                    "asset_id": asset["asset_id"],
                    "name": asset.get("name"),
                    "sum_insured": asset.get("sum_insured"),
                    "events": hits,
                }
            )
    return {"matches": matches, "parameters": {"max_distance_km": max_d, "min_magnitude": min_m}}


async def compute_parametric_payout(args: dict) -> dict:
    ev = args["event"]
    lat, lon, mag = require_event_coords(ev, "event")
    min_m = float(args["min_magnitude_trigger"])
    max_d = float(args["max_distance_km_trigger"])
    curve = args.get("payout_curve") or DEFAULT_PAYOUT_CURVE
    curve = sorted(curve, key=lambda b: b["max_distance_km"])
    for b in curve:
        if not 0.0 <= b["payout_percent"] <= 1.0:
            raise rpc_error(INVALID_PARAMS, "payout_curve.payout_percent must be between 0.0 and 1.0")

    triggered = mag is not None and mag >= min_m
    payouts, total = [], 0.0
    for asset in args["assets"]:
        d = haversine_km(lat, lon, float(asset["latitude"]), float(asset["longitude"]))
        pct = 0.0
        if triggered and d <= max_d:
            for band in curve:
                if d <= band["max_distance_km"]:
                    pct = float(band["payout_percent"])
                    break
        amount = round(float(asset["sum_insured"]) * pct, 2)
        total += amount
        payouts.append(
            {
                "asset_id": asset["asset_id"],
                "name": asset.get("name"),
                "distance_km": round(d, 2),
                "sum_insured": asset["sum_insured"],
                "payout_percent": pct,
                "payout_amount": amount,
            }
        )
    return {
        "event_id": ev.get("id"),
        "event_magnitude": mag,
        "parameters": {
            "min_magnitude_trigger": min_m,
            "max_distance_km_trigger": max_d,
            "payout_curve": curve,
        },
        "asset_payouts": payouts,
        "total_payout": round(total, 2),
    }


HANDLERS = {
    "usgs_search_earthquakes": usgs_search_earthquakes,
    "tmd_get_daily_earthquakes": tmd_get_daily_earthquakes,
    "tmd_get_recent_earthquakes": tmd_get_recent_earthquakes,
    "match_events_to_assets": match_events_to_assets,
    "compute_parametric_payout": compute_parametric_payout,
}

# ---------------------------------------------------------------------------
# MCP server wiring
# ---------------------------------------------------------------------------

server = Server(SERVER_NAME, version=SERVER_VERSION)


@server.list_tools()
async def list_tools() -> list[types.Tool]:
    return [types.Tool(name=t["name"], description=t["description"], inputSchema=t["inputSchema"]) for t in TOOLS]


async def _call_tool(req: types.CallToolRequest) -> types.ServerResult:
    """Custom tools/call handler so validation/HTTP/parse failures surface as JSON-RPC errors (per spec)."""
    name = req.params.name
    log.info("tools/call %s", name)
    args = validate_args(name, req.params.arguments)  # raises McpError(-32602)
    try:
        result = await HANDLERS[name](args)
    except McpError:
        raise
    except Exception as e:  # noqa: BLE001
        log.exception("tool %s failed", name)
        raise rpc_error(INTERNAL_ERROR, f"{type(e).__name__}: {e}") from None
    return types.ServerResult(
        types.CallToolResult(
            content=[types.TextContent(type="text", text=json.dumps(result, ensure_ascii=False))],
            structuredContent=result,
            isError=False,
        )
    )


# Registering directly replaces the SDK's default call_tool wrapper (which converts errors to isError results).
server.request_handlers[types.CallToolRequest] = _call_tool


# ---------------------------------------------------------------------------
# Transports
# ---------------------------------------------------------------------------


def build_http_app():
    from mcp.server.streamable_http_manager import StreamableHTTPSessionManager
    from starlette.applications import Starlette
    from starlette.middleware.cors import CORSMiddleware
    from starlette.requests import Request
    from starlette.responses import JSONResponse
    from starlette.routing import Route

    session_manager = StreamableHTTPSessionManager(
        app=server,
        stateless=os.getenv("MCP_STATELESS", "true").lower() in ("1", "true", "yes"),
        json_response=os.getenv("MCP_JSON_RESPONSE", "false").lower() in ("1", "true", "yes"),
    )

    class MCPEndpoint:
        async def __call__(self, scope, receive, send):
            await session_manager.handle_request(scope, receive, send)

    async def health(_: Request) -> JSONResponse:
        return JSONResponse({"status": "ok", "server": SERVER_NAME, "version": SERVER_VERSION})

    @contextlib.asynccontextmanager
    async def lifespan(_app):
        async with session_manager.run():
            log.info("Streamable HTTP MCP endpoint ready at /mcp")
            yield

    app = Starlette(
        routes=[Route("/health", health), Route("/mcp", MCPEndpoint()), Route("/mcp/", MCPEndpoint())],
        lifespan=lifespan,
    )
    return CORSMiddleware(
        app,
        allow_origins=os.getenv("CORS_ALLOW_ORIGINS", "*").split(","),
        allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
        allow_headers=["*"],
        expose_headers=["Mcp-Session-Id"],
    )


async def run_stdio() -> None:
    from mcp.server.stdio import stdio_server

    async with stdio_server() as (read, write):
        await server.run(read, write, server.create_initialization_options())


def main() -> None:
    transport = os.getenv("MCP_TRANSPORT", "streamable-http").lower()
    if transport == "stdio":
        asyncio.run(run_stdio())
        return
    import uvicorn

    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "8000"))
    log.info("Starting %s %s (streamable-http) on %s:%d", SERVER_NAME, SERVER_VERSION, host, port)
    uvicorn.run(build_http_app(), host=host, port=port, log_config=None)


if __name__ == "__main__":
    main()
