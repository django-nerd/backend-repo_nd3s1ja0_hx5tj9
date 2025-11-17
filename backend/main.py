import os
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from pytz import timezone
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from database import (
    create_document,
    get_documents,
    get_document,
    update_document,
    append_array_field,
    now_tz,
)
from schemas import (
    Lead, LeadCreate, LeadUpdate, LeadLog, WebhookIn, ManualLeadIn,
    WhatsAppMessageIn, SummaryConfig
)

import gspread
from google.oauth2.service_account import Credentials

logger = logging.getLogger("pk.app")
logging.basicConfig(level=logging.INFO)

APP_TZ = timezone(os.getenv("APP_TIMEZONE", "Asia/Kuala_Lumpur"))
KEYWORDS = [
    "paip", "wiring", "renovate", "leaking", "kontraktor",
    "plumber", "electrical", "bumbung"
]

DEFAULT_ADMIN = os.getenv("DEFAULT_ADMIN_WHATSAPP", "+60123456789")
GOOGLE_SHEET_NAME = os.getenv("GOOGLE_SHEET_NAME", "PK Leads Master")
GOOGLE_LOG_SHEET_NAME = os.getenv("GOOGLE_LOG_SHEET_NAME", "PK Lead Logs")

# Google Sheets setup: expects service account JSON path in GOOGLE_APPLICATION_CREDENTIALS
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]


def _get_gs_client():
    creds = Credentials.from_service_account_file(os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "service-account.json"), scopes=SCOPES)
    return gspread.authorize(creds)


def _append_to_sheet(row: List[Any]):
    try:
        gc = _get_gs_client()
        sh = gc.open(GOOGLE_SHEET_NAME)
        ws = sh.sheet1
        ws.append_row(row, value_input_option="USER_ENTERED")
    except Exception as e:
        logger.error("Failed to append to Google Sheet: %s", e)


def _append_log(row: List[Any]):
    try:
        gc = _get_gs_client()
        sh = gc.open(GOOGLE_SHEET_NAME)
        try:
            ws = sh.worksheet(GOOGLE_LOG_SHEET_NAME)
        except gspread.exceptions.WorksheetNotFound:
            ws = sh.add_worksheet(title=GOOGLE_LOG_SHEET_NAME, rows="1000", cols="10")
        ws.append_row(row, value_input_option="USER_ENTERED")
    except Exception as e:
        logger.error("Failed to append to Google Log Sheet: %s", e)


def _sheet_row_from_lead(lead: Dict[str, Any]) -> List[Any]:
    return [
        lead.get("name"),
        lead.get("phone"),
        lead.get("area"),
        lead.get("job_category"),
        lead.get("description"),
        lead.get("source"),
        lead.get("timestamp").astimezone(APP_TZ).strftime("%Y-%m-%d %H:%M:%S"),
        lead.get("status"),
        lead.get("assigned_sales_whatsapp"),
        lead.get("_id"),
    ]


# Simulated WhatsApp sender; replace with real provider API if available
import requests

WHATSAPP_API_URL = os.getenv("WHATSAPP_API_URL")  # optional external gateway
WHATSAPP_API_TOKEN = os.getenv("WHATSAPP_API_TOKEN")


def send_whatsapp_message(to_number: str, message: str) -> None:
    if not to_number:
        to_number = DEFAULT_ADMIN
    logger.info("WhatsApp → %s: %s", to_number, message)
    if WHATSAPP_API_URL and WHATSAPP_API_TOKEN:
        try:
            resp = requests.post(
                WHATSAPP_API_URL,
                headers={"Authorization": f"Bearer {WHATSAPP_API_TOKEN}", "Content-Type": "application/json"},
                json={"to": to_number, "message": message},
                timeout=10,
            )
            resp.raise_for_status()
        except Exception as e:
            logger.error("WhatsApp send failed: %s", e)


app = FastAPI(title="Pilih Kontraktor Lead Engine")


@app.get("/health")
async def health():
    return {"ok": True, "time": now_tz().astimezone(APP_TZ).isoformat()}


async def _log_status_change(lead_id: str, from_status: Optional[str], to_status: str, note: Optional[str] = None):
    log = LeadLog(
        lead_id=lead_id,
        from_status=from_status,
        to_status=to_status,
        timestamp=now_tz(),
        note=note,
    ).model_dump()
    await create_document("lead_log", log)
    _append_log([
        lead_id,
        from_status or "",
        to_status,
        log["timestamp"].astimezone(APP_TZ).strftime("%Y-%m-%d %H:%M:%S"),
        note or "",
    ])


async def _save_and_sheet(lead_data: Dict[str, Any]) -> Dict[str, Any]:
    lead_id = await create_document("lead", lead_data)
    lead = await get_document("lead", {"_id": lead_id})
    if lead:
        _append_to_sheet(_sheet_row_from_lead(lead))
    return lead or lead_data


# Normalize inputs

def normalize_lead(data: Dict[str, Any]) -> Dict[str, Any]:
    ts = data.get("timestamp") or now_tz()
    assigned = data.get("assigned_sales_whatsapp") or os.getenv("DEFAULT_ASSIGNED_WHATSAPP")
    return {
        "name": data.get("name") or "Unknown",
        "phone": data.get("phone") or "",
        "area": data.get("area"),
        "job_category": data.get("job_category"),
        "description": data.get("description"),
        "source": data.get("source", "manual"),
        "timestamp": ts,
        "status": "New",
        "assigned_sales_whatsapp": assigned if assigned else os.getenv("DEFAULT_ADMIN_WHATSAPP", DEFAULT_ADMIN),
        "last_sales_reply_at": None,
    }


# Inbound from website webhook
@app.post("/ingest/webhook")
async def ingest_webhook(payload: WebhookIn):
    data = normalize_lead({**payload.model_dump(exclude_none=True), "source": "website"})
    lead = await _save_and_sheet(data)
    # Alert assigned number immediately for visibility
    send_whatsapp_message(lead.get("assigned_sales_whatsapp"), f"New lead from website: {lead.get('name')} ({lead.get('phone')}). Status: New")
    return {"ok": True, "lead": lead}


# Inbound from Facebook Lead Ads
@app.post("/ingest/facebook")
async def ingest_facebook(payload: WebhookIn):
    data = normalize_lead({**payload.model_dump(exclude_none=True), "source": "facebook"})
    lead = await _save_and_sheet(data)
    send_whatsapp_message(lead.get("assigned_sales_whatsapp"), f"New Facebook lead: {lead.get('name')} ({lead.get('phone')}). Status: New")
    return {"ok": True, "lead": lead}


# Manual form entry
@app.post("/ingest/manual")
async def ingest_manual(payload: ManualLeadIn):
    data = normalize_lead({**payload.model_dump(exclude_none=True), "source": "manual"})
    lead = await _save_and_sheet(data)
    send_whatsapp_message(lead.get("assigned_sales_whatsapp"), f"Manual lead added: {lead.get('name')} ({lead.get('phone')}). Status: New")
    return {"ok": True, "lead": lead}


# WhatsApp inbound messages (filter by keywords to detect new enquiries)
@app.post("/ingest/whatsapp")
async def ingest_whatsapp(msg: WhatsAppMessageIn):
    text = msg.message.lower()
    if not any(k in text for k in KEYWORDS):
        # Treat as potential sales reply if matches existing lead assignment
        await update_document("lead", {"assigned_sales_whatsapp": msg.from_number}, {"last_sales_reply_at": msg.timestamp, "status": "In progress"})
        return {"ok": True, "ignored": True}

    # Create new lead detected from WhatsApp
    data = normalize_lead({
        "name": None,
        "phone": msg.from_number,
        "area": None,
        "job_category": None,
        "description": msg.message,
        "source": "whatsapp",
        "timestamp": msg.timestamp,
        "assigned_sales_whatsapp": msg.to_number,
    })
    lead = await _save_and_sheet(data)
    send_whatsapp_message(lead.get("assigned_sales_whatsapp"), f"New WhatsApp lead: {lead.get('phone')} → '{msg.message[:80]}'")
    return {"ok": True, "lead": lead}


# Update status explicitly
class StatusUpdateIn(BaseModel):
    lead_id: str
    status: str
    note: Optional[str] = None


@app.post("/lead/status")
async def update_status(payload: StatusUpdateIn):
    lead = await get_document("lead", {"_id": payload.lead_id})
    if not lead:
        raise HTTPException(404, "Lead not found")
    prev = lead.get("status")
    await update_document("lead", {"_id": payload.lead_id}, {"status": payload.status})
    await _log_status_change(payload.lead_id, prev, payload.status, payload.note)
    return {"ok": True}


# Scheduler for follow-ups and daily summaries
scheduler = AsyncIOScheduler(timezone=str(APP_TZ))


async def follow_up_checks():
    now = now_tz()
    # 15-minute no-reply alerts
    fifteen_ago = now - timedelta(minutes=15)
    leads = await get_documents("lead", {"status": "New"}, limit=5000)
    for lead in leads:
        if lead.get("timestamp") <= fifteen_ago and not lead.get("last_sales_reply_at"):
            send_whatsapp_message(lead.get("assigned_sales_whatsapp") or DEFAULT_ADMIN,
                                  f"Reminder: Lead {lead.get('name') or lead.get('phone')} has no reply after 15 min.")

    # 24-hour auto task (we log it as a note)
    day_ago = now - timedelta(hours=24)
    for lead in leads:
        if lead.get("timestamp") <= day_ago and lead.get("status") == "New":
            await _log_status_change(lead.get("_id"), "New", "New", "Auto follow-up task created after 24h of no response")
            send_whatsapp_message(lead.get("assigned_sales_whatsapp") or DEFAULT_ADMIN,
                                  f"Follow-up task: Lead {lead.get('name') or lead.get('phone')} still New after 24h.")


async def daily_summary_job():
    # Gather stats grouped by area and job_category
    leads = await get_documents("lead", {}, limit=10000)
    total = len(leads)
    by_status: Dict[str, int] = {"New": 0, "In progress": 0, "Won": 0, "Lost": 0}
    by_area: Dict[str, int] = {}
    by_job: Dict[str, int] = {}
    for l in leads:
        by_status[l.get("status", "New")] = by_status.get(l.get("status", "New"), 0) + 1
        area = (l.get("area") or "Unknown").title()
        by_area[area] = by_area.get(area, 0) + 1
        job = (l.get("job_category") or "Unknown").title()
        by_job[job] = by_job.get(job, 0) + 1

    # Conversion New → In progress (count leads that moved)
    logs = await get_documents("lead_log", {"from_status": "New", "to_status": "In progress"}, limit=100000)
    conversion = len(logs)

    # Overdue follow-ups (New older than 24h)
    day_ago = now_tz() - timedelta(hours=24)
    overdue = [l for l in leads if l.get("status") == "New" and l.get("timestamp") <= day_ago]

    lines = [
        "PK Daily Lead Summary",
        f"Total leads: {total}",
        f"Status → New: {by_status['New']}, In progress: {by_status['In progress']}, Won: {by_status['Won']}, Lost: {by_status['Lost']}",
        "By Area: " + ", ".join([f"{k}: {v}" for k, v in sorted(by_area.items())]),
        "By Job: " + ", ".join([f"{k}: {v}" for k, v in sorted(by_job.items())]),
        f"Conversion New→In progress: {conversion}",
        f"Overdue follow-ups (24h): {len(overdue)}",
    ]

    message = "\n".join(lines)
    # Send to default admin; in future can broadcast to team
    send_whatsapp_message(DEFAULT_ADMIN, message)


# Schedule: follow-up checks every 5 minutes; daily summary at 9 AM
scheduler.add_job(follow_up_checks, "interval", minutes=5, id="followups")
scheduler.add_job(daily_summary_job, CronTrigger(hour=9, minute=0), id="daily-summary")
scheduler.start()


# Simple frontend helper endpoints
class ManualFormIn(BaseModel):
    name: str
    phone: str
    area: Optional[str] = None
    job_category: Optional[str] = None
    description: Optional[str] = None
    assigned_sales_whatsapp: Optional[str] = None


@app.post("/leads")
async def create_lead(payload: ManualFormIn):
    data = normalize_lead({**payload.model_dump(exclude_none=True), "source": "manual"})
    lead = await _save_and_sheet(data)
    send_whatsapp_message(lead.get("assigned_sales_whatsapp"), f"New lead: {lead.get('name')} ({lead.get('phone')}). Status: New")
    return {"ok": True, "lead": lead}


@app.get("/leads")
async def list_leads():
    leads = await get_documents("lead", {}, limit=1000, sort=[("created_at", -1)])
    return {"ok": True, "leads": leads}
