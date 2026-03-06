"""ETL pipeline: fetch data from the autochecker API and load it into the database."""

from datetime import datetime
import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Dict, List, Any, Optional, Tuple

from app import models
from app.settings import settings


# ---------------------------------------------------------------------------
# Extract — fetch data from the autochecker API
# ---------------------------------------------------------------------------


async def fetch_items() -> list[dict]:
    """Fetch the lab/task catalog from the autochecker API."""
    async with httpx.AsyncClient() as client:
        response = await client.get(
            f"{settings.autochecker_api_url}/api/items",
            auth=(settings.autochecker_email, settings.autochecker_password),
            timeout=30.0,
        )
        response.raise_for_status()
        return response.json()


async def fetch_logs(since: datetime | None = None) -> list[dict]:
    """Fetch check results from the autochecker API with pagination."""
    all_logs = []
    current_since = since
    limit = 500

    async with httpx.AsyncClient() as client:
        while True:
            params = {"limit": limit}
            if current_since:
                params["since"] = current_since.isoformat()

            response = await client.get(
                f"{settings.autochecker_api_url}/api/logs",
                auth=(settings.autochecker_email, settings.autochecker_password),
                params=params,
                timeout=30.0,
            )
            response.raise_for_status()
            data = response.json()

            logs = data.get("logs", [])
            all_logs.extend(logs)

            if not data.get("has_more", False):
                break

            if logs:
                last_log = logs[-1]
                current_since = datetime.fromisoformat(
                    last_log["submitted_at"].replace("Z", "+00:00")
                )

    return all_logs


# ---------------------------------------------------------------------------
# Load — insert fetched data into the local database
# ---------------------------------------------------------------------------


async def load_items(items: list[dict], session: AsyncSession) -> int:
    """Load items (labs and tasks) into the database."""
    new_count = 0
    lab_map = {}

    # Process labs
    for item in items:
        if item["type"] == "lab":
            stmt = select(models.Item).where(
                models.Item.type == "lab", models.Item.title == item["title"]
            )
            result = await session.execute(stmt)
            existing = result.scalar_one_or_none()

            if not existing:
                db_item = models.Item(type="lab", title=item["title"])
                session.add(db_item)
                await session.flush()
                lab_map[item["lab"]] = db_item
                new_count += 1
            else:
                lab_map[item["lab"]] = existing

    # Process tasks
    for item in items:
        if item["type"] == "task":
            parent_lab = lab_map.get(item["lab"])
            if not parent_lab:
                continue

            stmt = select(models.Item).where(
                models.Item.type == "task",
                models.Item.title == item["title"],
                models.Item.parent_id == parent_lab.id,
            )
            result = await session.execute(stmt)
            existing = result.scalar_one_or_none()

            if not existing:
                db_item = models.Item(
                    type="task", title=item["title"], parent_id=parent_lab.id
                )
                session.add(db_item)
                new_count += 1

    await session.commit()
    return new_count


async def load_logs(
    logs: list[dict], items_catalog: list[dict], session: AsyncSession
) -> int:
    """Load interaction logs into the database."""
    # Build lookup from (lab_short_id, task_short_id) to item title
    title_lookup = {}
    for item in items_catalog:
        if item["type"] == "lab":
            title_lookup[(item["lab"], None)] = item["title"]
        else:
            title_lookup[(item["lab"], item["task"])] = item["title"]

    new_count = 0

    for log in logs:
        # Find or create learner
        stmt = select(models.Learner).where(
            models.Learner.external_id == log["student_id"]
        )
        result = await session.execute(stmt)
        learner = result.scalar_one_or_none()

        if not learner:
            learner = models.Learner(
                external_id=log["student_id"], group=log.get("group", "unknown")
            )
            session.add(learner)
            await session.flush()

        # Find the matching item
        item_key = (log["lab"], log.get("task"))
        item_title = title_lookup.get(item_key)

        if not item_title:
            continue

        stmt = select(models.Item).where(models.Item.title == item_title)
        result = await session.execute(stmt)
        item = result.scalar_one_or_none()

        if not item:
            continue

        # Check if log already exists
        stmt = select(models.InteractionLog).where(
            models.InteractionLog.external_id == str(log["id"])
        )
        result = await session.execute(stmt)
        existing = result.scalar_one_or_none()

        if existing:
            continue

        # Create new interaction
        submitted_at = datetime.fromisoformat(
            log["submitted_at"].replace("Z", "+00:00")
        )

        interaction = models.InteractionLog(
            external_id=str(log["id"]),
            learner_id=learner.id,
            item_id=item.id,
            kind="attempt",
            score=log["score"],
            checks_passed=log["passed"],
            checks_total=log["total"],
            created_at=submitted_at,
        )
        session.add(interaction)
        new_count += 1

    await session.commit()
    return new_count


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


async def sync(session: AsyncSession) -> dict:
    """Run the full ETL pipeline."""
    items_data = await fetch_items()
    new_items = await load_items(items_data, session)

    stmt = (
        select(models.InteractionLog)
        .order_by(models.InteractionLog.created_at.desc())
        .limit(1)
    )
    result = await session.execute(stmt)
    last_log = result.scalar_one_or_none()

    since = last_log.created_at if last_log else None

    logs_data = await fetch_logs(since)
    new_logs = await load_logs(logs_data, items_data, session)

    stmt = select(models.InteractionLog)
    result = await session.execute(stmt)
    total_logs = len(result.scalars().all())

    return {"new_records": new_items + new_logs, "total_records": total_logs}
