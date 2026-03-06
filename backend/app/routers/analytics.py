from fastapi import APIRouter, Depends, Query
from sqlmodel.ext.asyncio.session import AsyncSession
from sqlmodel import select, func, case, distinct
from typing import List, Dict, Any

from app.database import get_session
from app.models.learner import Learner
from app.models.interaction import InteractionLog
from app.models.item import ItemRecord

router = APIRouter()


def lab_id_to_title(lab_id: str) -> str:
    parts = lab_id.split("-")
    if len(parts) == 2 and parts[0] == "lab":
        return f"Lab {parts[1]}"
    return lab_id


@router.get("/scores")
async def get_scores(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    lab_title = lab_id_to_title(lab)
    lab_stmt = select(ItemRecord).where(
        ItemRecord.type == "lab", ItemRecord.title.contains(lab_title)
    )
    lab_res = await session.exec(lab_stmt)
    lab_item = lab_res.first()

    if not lab_item:
        return [{"bucket": b, "count": 0} for b in ["0-25", "26-50", "51-75", "76-100"]]

    stmt = (
        select(
            case(
                (InteractionLog.score <= 25, "0-25"),
                (InteractionLog.score <= 50, "26-50"),
                (InteractionLog.score <= 75, "51-75"),
                else_="76-100",
            ).label("bucket"),
            func.count(InteractionLog.id).label("count"),
        )
        .join(ItemRecord, InteractionLog.item_id == ItemRecord.id)
        .where(ItemRecord.parent_id == lab_item.id)
        .group_by("bucket")
    )

    result = await session.exec(stmt)
    rows = {row.bucket: row.count for row in result.all()}

    return [
        {"bucket": b, "count": rows.get(b, 0)}
        for b in ["0-25", "26-50", "51-75", "76-100"]
    ]


@router.get("/pass-rates")
async def get_pass_rates(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    lab_title = lab_id_to_title(lab)
    lab_stmt = select(ItemRecord).where(
        ItemRecord.type == "lab", ItemRecord.title.contains(lab_title)
    )
    lab_res = await session.exec(lab_stmt)
    lab_item = lab_res.first()

    if not lab_item:
        return []

    stmt = (
        select(
            ItemRecord.title.label("task"),
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count(InteractionLog.id).label("attempts"),
        )
        .join(InteractionLog, ItemRecord.id == InteractionLog.item_id)
        .where(ItemRecord.parent_id == lab_item.id)
        .group_by(ItemRecord.id, ItemRecord.title)
        .order_by(ItemRecord.title)
    )

    result = await session.exec(stmt)
    return [
        {"task": row.task, "avg_score": row.avg_score, "attempts": row.attempts}
        for row in result.all()
    ]


@router.get("/timeline")
async def get_timeline(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    lab_title = lab_id_to_title(lab)
    lab_stmt = select(ItemRecord).where(
        ItemRecord.type == "lab", ItemRecord.title.contains(lab_title)
    )
    lab_res = await session.exec(lab_stmt)
    lab_item = lab_res.first()

    if not lab_item:
        return []

    stmt = (
        select(
            func.date(InteractionLog.created_at).label("date"),
            func.count(InteractionLog.id).label("submissions"),
        )
        .join(ItemRecord, InteractionLog.item_id == ItemRecord.id)
        .where(ItemRecord.parent_id == lab_item.id)
        .group_by(func.date(InteractionLog.created_at))
        .order_by("date")
    )

    result = await session.exec(stmt)
    return [
        {"date": str(row.date), "submissions": row.submissions} for row in result.all()
    ]


@router.get("/groups")
async def get_groups(
    lab: str = Query(..., description="Lab identifier, e.g. 'lab-01'"),
    session: AsyncSession = Depends(get_session),
):
    lab_title = lab_id_to_title(lab)
    lab_stmt = select(ItemRecord).where(
        ItemRecord.type == "lab", ItemRecord.title.contains(lab_title)
    )
    lab_res = await session.exec(lab_stmt)
    lab_item = lab_res.first()

    if not lab_item:
        return []

    # Используем getattr для безопасности, если поле в БД называется иначе
    # Но судя по тестам, оно должно называться student_group или просто group
    group_attr = getattr(Learner, "student_group", getattr(Learner, "group", None))

    stmt = (
        select(
            group_attr.label("name"),
            func.round(func.avg(InteractionLog.score), 1).label("avg_score"),
            func.count(distinct(Learner.id)).label("students"),
        )
        .join(InteractionLog, Learner.id == InteractionLog.learner_id)
        .join(ItemRecord, InteractionLog.item_id == ItemRecord.id)
        .where(ItemRecord.parent_id == lab_item.id)
        .group_by(group_attr)
        .order_by(group_attr)
    )

    result = await session.exec(stmt)
    return [
        {"group": row.name, "avg_score": row.avg_score, "students": row.students}
        for row in result.all()
    ]
