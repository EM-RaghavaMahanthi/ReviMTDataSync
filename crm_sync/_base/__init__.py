"""
Shared helpers for JSON:API relationship parsing.
"""

from urllib.parse import urlparse


def extract_tenant_name(api_base_url: str):
    """
    Subdomain of the tenant's crm_api_end_point, e.g.
    https://modernpilatesstudio.marianatek.com/api -> "modernpilatesstudio".
    Used for customer_tags_default, which is keyed by (tenant_name, name) rather
    than account_id (default/system tags are shared across accounts on one tenant).
    """
    if not api_base_url:
        return None
    host = urlparse(api_base_url).netloc
    return host.split(".")[0] if host else None


def extract_rel_id(rel):
    """Extract id from a JSON:API relationship data field."""
    if not rel:
        return None
    data = rel.get("data")
    if isinstance(data, dict):
        return data.get("id")
    if isinstance(data, list) and data:
        return data[0].get("id")
    return None


def extract_rel_type(rel):
    """Extract type from a JSON:API relationship data field."""
    if not rel:
        return None
    data = rel.get("data")
    if isinstance(data, dict):
        return data.get("type")
    if isinstance(data, list) and data:
        return data[0].get("type")
    return None
