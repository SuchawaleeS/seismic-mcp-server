# Seismic MCP Server (Streamable HTTP + Docker)

MCP server สำหรับข้อมูลแผ่นดินไหว (USGS + TMD) และเครื่องมือวิเคราะห์ประกันภัยแบบ parametric
ตาม `spec.md` แต่ใช้ transport แบบ **Streamable HTTP** (endpoint `/mcp`) และรันใน Docker

## Tools
| Tool | คำอธิบาย |
|---|---|
| `usgs_search_earthquakes` | ค้นหาแผ่นดินไหวจาก USGS FDSN |
| `tmd_get_daily_earthquakes` | แผ่นดินไหวรายวันจาก TMD (default = วันนี้ ตาม `TZ`) |
| `tmd_get_recent_earthquakes` | แผ่นดินไหวย้อนหลัง N วัน (1–7) จาก TMD |
| `match_events_to_assets` | จับคู่เหตุการณ์กับทรัพย์สินที่เอาประกันตามระยะ/ขนาด |
| `compute_parametric_payout` | คำนวณ payout แบบ parametric ตาม payout curve |

ทุก tool ที่คืนแผ่นดินไหวใช้ schema `EarthquakeEvent` ตาม spec
ผลลัพธ์ส่งทั้ง `structuredContent` และ `content[0].text` (JSON string)
argument ผิด schema → JSON-RPC error `-32602`; HTTP/parse error จาก upstream → `-32000`

## รันด้วย Docker

```bash
docker compose up -d --build
```

```bash
curl http://localhost:8000/health
```

หรือไม่ใช้ compose:

```bash
docker build -t seismic-mcp-server:0.1.0 . && docker run -d -p 8000:8000 --name seismic-mcp seismic-mcp-server:0.1.0
```

MCP endpoint: `http://localhost:8000/mcp`

### Environment variables
| ตัวแปร | Default | ความหมาย |
|---|---|---|
| `PORT` / `HOST` | `8000` / `0.0.0.0` | ที่อยู่ที่ฟัง |
| `TZ` | `Asia/Bangkok` | timezone ที่ใช้นิยาม "วันนี้" ของ tool TMD |
| `MCP_STATELESS` | `true` | stateless (ไม่ต้องมี session affinity, scale ได้ง่าย) |
| `MCP_JSON_RESPONSE` | `false` | `true` = ตอบเป็น JSON ธรรมดาแทน SSE |
| `MCP_TRANSPORT` | `streamable-http` | ตั้งเป็น `stdio` เพื่อใช้แบบ stdio เดิม |
| `CORS_ALLOW_ORIGINS` | `*` | origin ที่อนุญาต (คั่นด้วย comma) |
| `LOG_LEVEL` | `INFO` | log ไป stderr เท่านั้น |

## ทดสอบ

```bash
docker run --rm -v "$PWD/test_client.py:/t.py:ro" seismic-mcp-server:0.1.0 python /t.py http://host.docker.internal:8000/mcp
```

หรือด้วย curl:

```bash
curl -s -X POST http://localhost:8000/mcp -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
```

## เชื่อมกับ client

**Claude Code**

```bash
claude mcp add --transport http seismic http://localhost:8000/mcp
```

**Claude Desktop** (ผ่าน `mcp-remote` bridge, ใน `claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "seismic-mcp": {
      "command": "npx",
      "args": ["-y", "mcp-remote", "http://localhost:8000/mcp"]
    }
  }
}
```

**MCP Inspector**: `npx @modelcontextprotocol/inspector` → เลือก transport "Streamable HTTP" → URL `http://localhost:8000/mcp`

## หมายเหตุเกี่ยวกับ TMD API (ต่างจาก spec)
ผลจริงจาก TMD demo API ต่างจากตัวอย่างใน spec โค้ดรองรับทั้งสองแบบ:
- key ของ array คือ `DailyEarthquakes` (spec: `DailySeismicEvent`)
- ไม่มี `EventID` → สร้าง id คงที่เป็น `TMD-<sha1(DateTimeUTC|lat|lon)[:12]>`
- เวลาอยู่ใน `DateTimeUTC` / `DateTimeThai` → แปลงเป็น ISO8601 UTC (`...Z`)
- ตัวเลขเป็น string, ความลึกคือ `Depth`, สถานที่คือ `OriginThai`
- API **ไม่สนใจพารามิเตอร์ `Date`** และคืนประวัติยาว (~1,300+ รายการ) → server กรองตามวันที่ตามเวลาไทยเอง
