"""Smoke test: python test_client.py [http://localhost:8000/mcp]"""

import asyncio
import json
import sys

from mcp import ClientSession
from mcp.client.streamable_http import streamablehttp_client
from mcp.shared.exceptions import McpError

URL = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8000/mcp"


async def call(session: ClientSession, name: str, args: dict) -> dict:
    res = await session.call_tool(name, args)
    return res.structuredContent or json.loads(res.content[0].text)


async def main() -> None:
    async with streamablehttp_client(URL) as (read, write, _):
        async with ClientSession(read, write) as session:
            init = await session.initialize()
            print("server:", init.serverInfo.name, init.serverInfo.version, "| protocol", init.protocolVersion)

            tools = await session.list_tools()
            print("tools:", [t.name for t in tools.tools])

            usgs = await call(session, "usgs_search_earthquakes",
                              {"starttime": "2026-03-01", "endtime": "2026-03-07", "minmagnitude": 4.5, "limit": 5})
            print(f"usgs: {len(usgs['events'])} events; first:",
                  {k: usgs['events'][0][k] for k in ('id', 'time', 'place', 'magnitude')} if usgs['events'] else None)

            daily = await call(session, "tmd_get_daily_earthquakes", {})
            print(f"tmd daily ({daily['query']['date']}): {len(daily['events'])} events")

            recent = await call(session, "tmd_get_recent_earthquakes", {"days": 3})
            print(f"tmd recent (3d): {len(recent['events'])} events;",
                  [(e['time'], e['magnitude'], e['place']) for e in recent['events'][:3]])

            assets = [
                {"asset_id": "ASSET-001", "name": "Warehouse A", "latitude": 18.79, "longitude": 98.98, "sum_insured": 1_000_000},
                {"asset_id": "ASSET-002", "name": "Factory B", "latitude": 13.75, "longitude": 100.5, "sum_insured": 5_000_000},
            ]
            event = {"id": "TEST-1", "time": "2026-03-07T09:30:15+07:00", "place": "Near Chiang Mai",
                     "magnitude": 5.7, "latitude": 18.5, "longitude": 98.8, "depth_km": 10,
                     "source": "TMD_DailySeismicEvent", "url": None, "raw": {}}

            m = await call(session, "match_events_to_assets", {"events": [event], "assets": assets})
            print("matches:", [(x["asset_id"], x["events"][0]["distance_km"]) for x in m["matches"]])

            p = await call(session, "compute_parametric_payout", {"event": event, "assets": assets})
            print("payouts:", [(a["asset_id"], a["distance_km"], a["payout_percent"], a["payout_amount"])
                               for a in p["asset_payouts"]], "total:", p["total_payout"])

            try:
                await session.call_tool("usgs_search_earthquakes", {"starttime": "2026-03-01"})
                print("validation: UNEXPECTED success")
            except McpError as e:
                print("validation error (expected):", e.error.code, e.error.message)


asyncio.run(main())
