from dateutil.parser import isoparse


def validate_iso_8601_timestamp(timestamp: str) -> bool:
    try:
        isoparse(timestamp)
        return True
    except Exception:
        return False
