from typing import Optional, Literal, List
from pydantic import BaseModel, Field
from datetime import datetime

# Lead schema => collection name "lead"
Status = Literal["New", "In progress", "Won", "Lost"]

class Lead(BaseModel):
    name: str
    phone: str
    area: Optional[str] = None
    job_category: Optional[str] = Field(None, description="e.g., plumbing, electrical")
    description: Optional[str] = None
    source: Literal["whatsapp", "website", "facebook", "manual"]
    timestamp: datetime
    status: Status = "New"
    assigned_sales_whatsapp: Optional[str] = None
    # internal helper to track reply detection
    last_sales_reply_at: Optional[datetime] = None

class LeadCreate(Lead):
    pass

class LeadUpdate(BaseModel):
    status: Optional[Status] = None
    assigned_sales_whatsapp: Optional[str] = None
    last_sales_reply_at: Optional[datetime] = None

class LeadLog(BaseModel):
    lead_id: str
    from_status: Optional[Status] = None
    to_status: Status
    timestamp: datetime
    note: Optional[str] = None

class WebhookIn(BaseModel):
    name: Optional[str] = None
    phone: Optional[str] = None
    area: Optional[str] = None
    job_category: Optional[str] = None
    description: Optional[str] = None
    source: Optional[str] = None
    assigned_sales_whatsapp: Optional[str] = None

class ManualLeadIn(BaseModel):
    name: str
    phone: str
    area: Optional[str] = None
    job_category: Optional[str] = None
    description: Optional[str] = None
    assigned_sales_whatsapp: Optional[str] = None

class WhatsAppMessageIn(BaseModel):
    from_number: str
    to_number: str
    message: str
    timestamp: datetime

class SummaryConfig(BaseModel):
    default_admin_number: str
