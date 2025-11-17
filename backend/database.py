import os
import logging
from typing import Any, Dict, List, Optional
from datetime import datetime

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase
from pytz import timezone

logger = logging.getLogger("pk.database")
logger.setLevel(logging.INFO)

# MongoDB connection
MONGO_URL = os.getenv("DATABASE_URL", "mongodb://localhost:27017")
DB_NAME = os.getenv("DATABASE_NAME", "pk_leads")
TZ = timezone(os.getenv("APP_TIMEZONE", "Asia/Kuala_Lumpur"))

_client: Optional[AsyncIOMotorClient] = None
_db: Optional[AsyncIOMotorDatabase] = None


def get_db() -> AsyncIOMotorDatabase:
    global _client, _db
    if _db is None:
        _client = AsyncIOMotorClient(MONGO_URL)
        _db = _client[DB_NAME]
        logger.info("Connected to MongoDB at %s (db=%s)", MONGO_URL, DB_NAME)
    return _db


def now_tz() -> datetime:
    return datetime.now(TZ)


async def create_document(collection_name: str, data: Dict[str, Any]) -> str:
    db = get_db()
    data = {**data}
    ts = now_tz()
    data.setdefault("created_at", ts)
    data.setdefault("updated_at", ts)
    res = await db[collection_name].insert_one(data)
    return str(res.inserted_id)


async def update_document(collection_name: str, filter_dict: Dict[str, Any], update_dict: Dict[str, Any]) -> int:
    db = get_db()
    update_dict = {**update_dict, "updated_at": now_tz()}
    res = await db[collection_name].update_many(filter_dict, {"$set": update_dict})
    return res.modified_count


async def get_documents(collection_name: str, filter_dict: Optional[Dict[str, Any]] = None, limit: int = 1000, sort: Optional[List] = None) -> List[Dict[str, Any]]:
    db = get_db()
    cursor = db[collection_name].find(filter_dict or {})
    if sort:
        cursor = cursor.sort(sort)
    if limit:
        cursor = cursor.limit(limit)
    return [
        {**doc, "_id": str(doc.get("_id"))}
        async for doc in cursor
    ]


async def get_document(collection_name: str, filter_dict: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    db = get_db()
    doc = await db[collection_name].find_one(filter_dict)
    if doc:
        doc["_id"] = str(doc["_id"])  # type: ignore[index]
    return doc


async def append_array_field(collection_name: str, filter_dict: Dict[str, Any], array_field: str, item: Any) -> int:
    db = get_db()
    res = await db[collection_name].update_many(filter_dict, {"$push": {array_field: item}, "$set": {"updated_at": now_tz()}})
    return res.modified_count
