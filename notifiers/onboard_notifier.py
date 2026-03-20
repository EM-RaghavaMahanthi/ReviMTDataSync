import logging
from typing import Optional

from .clients.teams_client import TeamsClient, create_teams_client_from_config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _card(title: str, subtitle: str, facts: list, success: bool) -> dict:
    """Build a Teams MessageCard with a color-coded left border and a facts table."""
    return {
        "@type": "MessageCard",
        "@context": "http://schema.org/extensions",
        "themeColor": "00B050" if success else "FF0000",
        "summary": f"{title} — {subtitle}",
        "sections": [
            {
                "activityTitle": f"**{title}**",
                "activitySubtitle": subtitle,
                "facts": [{"name": k, "value": str(v)} for k, v in facts],
            }
        ],
    }


async def _send(client: TeamsClient, card: dict, op: str) -> bool:
    try:
        resp = await client.send_message(payload=card, operation_name=op)
        return resp.success
    except Exception as e:
        logger.error(f"[{op}] Teams notification failed: {e}")
        return False


# ---------------------------------------------------------------------------
# Stage 1 — CRM → S3
# ---------------------------------------------------------------------------

class Stage1Notifier:
    """Notifier for Stage 1: CRM API → S3."""

    def __init__(self, teams_client: Optional[TeamsClient] = None):
        self.client = teams_client or create_teams_client_from_config()

    async def notify(self, data: dict) -> bool:
        if not self.client:
            return False

        success = "success" in str(data.get("status", "")).lower()
        resource  = data.get("resource", "-")
        account   = data.get("account_id", "-")
        records   = data.get("total_records", 0)
        expected  = data.get("total_expected", records)
        missing   = data.get("missing_records", 0)
        elapsed   = data.get("elapsed", "-")

        subtitle = "✅ SUCCESS" if success else "❌ FAILED"
        facts = [
            ("Resource",  resource),
            ("Account",   account),
            ("Synced",    f"{records:,} / {expected:,} expected"),
        ]
        if missing:
            facts.append(("Missing", f"⚠ {missing:,}"))
        facts.append(("Time", elapsed))

        card = _card("Stage 1 — CRM → S3", subtitle, facts, success)
        return await _send(self.client, card, "stage1_crm_to_s3")

    async def close(self):
        if self.client:
            await self.client.close()


# ---------------------------------------------------------------------------
# Stage 2 — S3 → Staging DB
# ---------------------------------------------------------------------------

class Stage2Notifier:
    """Notifier for Stage 2: S3 → Staging DB."""

    def __init__(self, teams_client: Optional[TeamsClient] = None):
        self.client = teams_client or create_teams_client_from_config()

    async def notify(self, data: dict) -> bool:
        if not self.client:
            return False

        success       = data.get("etl_success", False)
        account       = data.get("account_id", "-")
        failed_tables = data.get("failed_tables", [])
        elapsed       = data.get("elapsed", "-")

        subtitle = "✅ SUCCESS" if success else "❌ FAILED"
        facts = [
            ("Account", account),
            ("Status",  subtitle),
        ]
        if failed_tables:
            facts.append(("Failed Tables", ", ".join(failed_tables)))
        facts.append(("Time", elapsed))

        card = _card("Stage 2 — S3 → Staging DB", subtitle, facts, success)
        return await _send(self.client, card, "stage2_s3_to_stg")

    async def close(self):
        if self.client:
            await self.client.close()


# ---------------------------------------------------------------------------
# Stage 3 — Staging → Main DB
# ---------------------------------------------------------------------------

class Stage3Notifier:
    """Notifier for Stage 3: Staging DB → Production DB."""

    def __init__(self, teams_client: Optional[TeamsClient] = None):
        self.client = teams_client or create_teams_client_from_config()

    async def notify(self, data: dict) -> bool:
        if not self.client:
            return False

        success       = data.get("status") == "success"
        account       = data.get("account_id", "-")
        success_count = data.get("success_count", 0)
        total_tables  = data.get("total_tables", 0)
        total_inserted = data.get("total_inserted", 0)
        failed_table  = data.get("failed_table")
        elapsed       = data.get("elapsed", "-")

        subtitle = "✅ SUCCESS" if success else "❌ FAILED"
        facts = [
            ("Account",         account),
            ("Tables",          f"{success_count} / {total_tables}"),
            ("Records Inserted", f"{total_inserted:,}"),
        ]
        if failed_table:
            facts.append(("Failed At", f"⚠ {failed_table}"))
        facts.append(("Time", elapsed))

        card = _card("Stage 3 — Staging → Main DB", subtitle, facts, success)
        return await _send(self.client, card, "stage3_stg_to_main")

    async def close(self):
        if self.client:
            await self.client.close()
