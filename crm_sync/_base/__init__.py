"""
Shared helpers for JSON:API relationship parsing.
"""


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
