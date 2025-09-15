from datetime import datetime
from enum import Enum
from typing import Generator

import ee

from aef_export.sqlite import update_row, get_summary


class BillingTier(Enum):
    tier1 = "tier1"
    tier2 = "tier2"
    tier3 = "tier3"


def list_tasks() -> Generator[dict, None, None]:
    tasks = ee.data.listOperations()
    for task in tasks:
        if task["metadata"]["type"] != "EXPORT_IMAGE":
            continue
        yield task


def update_db_state():
    for task in list_tasks():
        task_state = task["metadata"]["state"]
        task_id = task["name"].split("/")[-1]
        eecu_seconds = None
        duration_seconds = None
        if task_state == "SUCCEEDED":
            eecu_seconds = task["metadata"]["batchEecuUsageSeconds"]
            start_time = datetime.fromisoformat(task["metadata"]["startTime"])
            end_time = datetime.fromisoformat(task["metadata"]["endTime"])
            duration_seconds = (end_time - start_time).total_seconds()

        update_row(task_id, task_state, eecu_seconds, duration_seconds)


def get_task_summary(tier: BillingTier) -> list[dict]:
    billing_tiers = {
        BillingTier.tier1: 0.40,
        BillingTier.tier2: 0.28,
        BillingTier.tier3: 0.16,
    }

    summary = get_summary()

    out_rows = []
    for row in summary:
        if eecu_seconds := row.get("eecu_seconds"):
            eecu_hours = eecu_seconds / 3600
            row["compute_cost"] = billing_tiers[tier] * eecu_hours

        if avg_runtime_seconds := row.pop("avg_runtime_seconds"):
            row["avg_runtime_minutes"] = avg_runtime_seconds / 60
        out_rows.append(row)
    return out_rows
