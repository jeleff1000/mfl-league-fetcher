"""Storage failures shared by Fly reads and publications."""


def is_permanent_storage_error(detail: str) -> bool:
    """These failures require storage recovery, not another identical request."""
    detail = (detail or "").lower()
    return (
        "corrupt database file" in detail
        or ("computed checksum" in detail and "stored checksum" in detail)
        or "database has been invalidated" in detail
        or "current transaction is aborted" in detail
    )
