"""
Teams alert for the 3-hourly notes refresh (state machine refresh_customer_notes_marianatek_cron,
revi-backfill event_type=refresh_recent_notes).

Why this exists: a failed account is caught and recorded as status=FAILED so the other
accounts keep running. The trade-off is that both the Lambda and the execution report
success no matter what. From 11-13 to 11-16 every account failed on every run and nothing
alerted. The state machine now calls this once per run, after the Map. It posts only when an
account failed or the account list could not be loaded, so a healthy run sends nothing.

Failures are grouped by error message. An outage that hits every account produces one line,
"106 accounts: <error>", not 106 separate lines.

The card is an Adaptive Card wrapped in a message attachment. Teams Workflows webhooks (the
replacement for the retired Office 365 connectors) require this format, and the legacy
connectors accept it too. The MessageCard that onboard_notifier builds works only on the
legacy connectors.

Config (env on revi-backfill, read via core.config): TEAMS_ENABLED + TEAMS_WEBHOOK_URL.
If either is unset, nothing is posted and the failures are only logged.
"""

import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Optional

from .clients.teams_client import create_teams_client_from_config

logger = logging.getLogger(__name__)

_MAX_ACCOUNTS_LISTED = 15
_MAX_ERROR_CHARS = 300


def _short_error(err) -> str:
    """One readable line from whatever the state machine handed us.

    A Catch hands over the Lambda's Cause, which is itself a JSON string carrying
    errorMessage. A status=error result hands over the plain message. Either way, keep
    the first line: psycopg2 errors follow it with the SQL and a docs link.
    """
    if err is None:
        return "unknown error (no message recorded)"
    # Unwrap up to two layers: a Catch gives {"Error", "Cause"}, where Cause is itself JSON
    # carrying errorMessage; a status=error result gives {"status": "error", "error": "..."}.
    for _ in range(2):
        if not isinstance(err, str):
            break
        try:
            parsed = json.loads(err)
        except ValueError:
            break
        if not isinstance(parsed, dict):
            break
        err = (parsed.get("errorMessage") or parsed.get("error") or parsed.get("Cause")
               or parsed.get("Error") or err)
    text = str(err).strip().splitlines()[0] if str(err).strip() else "unknown error"
    return text[:_MAX_ERROR_CHARS]


def summarize(results: list) -> dict:
    failed = [r for r in results if isinstance(r, dict) and r.get("status") != "SUCCESS"]
    by_error = defaultdict(list)
    for r in failed:
        by_error[_short_error(r.get("error"))].append(str(r.get("account_id")))
    return {"total": len(results), "failed": len(failed), "by_error": dict(by_error)}


def _text(text: str, **kw) -> dict:
    return {"type": "TextBlock", "text": text, "wrap": True, **kw}


def build_card(execution_name: str, summary: dict, stage0_error: Optional[str]) -> dict:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    if stage0_error:
        title = "Notes refresh did not run: could not load the account list"
    else:
        title = f"Notes refresh: {summary['failed']} of {summary['total']} accounts failed"

    body = [
        _text(title, weight="Bolder", size="Medium", color="Attention"),
        {
            "type": "FactSet",
            "facts": [
                {"title": "Job", "value": "refresh_recent_notes (revi-backfill)"},
                {"title": "State machine", "value": "refresh_customer_notes_marianatek_cron"},
                {"title": "Execution", "value": execution_name},
                {"title": "When", "value": now},
            ],
        },
    ]

    if stage0_error:
        body.append(_text(_short_error(stage0_error), fontType="Monospace", isSubtle=True))
    for err, accounts in sorted(summary["by_error"].items(), key=lambda kv: -len(kv[1])):
        listed = ", ".join(accounts[:_MAX_ACCOUNTS_LISTED])
        if len(accounts) > _MAX_ACCOUNTS_LISTED:
            listed += f" and {len(accounts) - _MAX_ACCOUNTS_LISTED} more"
        noun = "account" if len(accounts) == 1 else "accounts"
        body.append(_text(f"**{len(accounts)} {noun}:** {listed}", spacing="Medium"))
        body.append(_text(err, fontType="Monospace", isSubtle=True, spacing="None"))

    body.append(_text(
        "Each run looks back NOTES_WINDOW_HOURS (24h). Notes older than that are not "
        "re-fetched automatically, but they are still in MarianaTek. Fix the error, then "
        'invoke revi-backfill with "since" set to the first failed run to refill the gap.',
        isSubtle=True, size="Small", spacing="Large",
    ))

    return {
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "contentUrl": None,
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.4",
                "msteams": {"width": "Full"},
                "body": body,
            },
        }],
    }


async def notify_refresh_failures(
    execution_name: str, results: Optional[list] = None, stage0_error: Optional[str] = None
) -> dict:
    summary = summarize(results or [])
    counts = {"accounts_total": summary["total"], "accounts_failed": summary["failed"]}

    if not stage0_error and summary["failed"] == 0:
        logger.info(f"[notify] {execution_name}: all {summary['total']} accounts succeeded — no alert")
        return {"notified": False, "reason": "no failures", **counts}

    for err, accounts in summary["by_error"].items():
        logger.error(f"[notify] {execution_name}: {len(accounts)} failed — {err} — accounts {accounts}")
    if stage0_error:
        logger.error(f"[notify] {execution_name}: stage 0 failed — {_short_error(stage0_error)}")

    client = create_teams_client_from_config()
    if client is None:
        logger.warning("[notify] Teams not configured (TEAMS_ENABLED / TEAMS_WEBHOOK_URL) — alert NOT sent")
        return {"notified": False, "reason": "teams not configured", **counts}

    try:
        resp = await client.send_message(
            build_card(execution_name, summary, stage0_error), operation_name="notes_refresh_failures"
        )
    finally:
        await client.close()

    if not resp.success:
        logger.error(f"[notify] Teams post failed: {resp.error_message}")
    return {"notified": resp.success, "reason": None if resp.success else resp.error_message, **counts}
