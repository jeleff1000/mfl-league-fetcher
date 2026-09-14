"""Bearer token authentication for DuckDB server."""

import os

from fastapi import Request, HTTPException


class AuthError(Exception):
    pass


def validate_read_token(token: str) -> bool:
    """Validate a read-level bearer token."""
    expected = os.environ.get("DATABASE_READ_TOKEN", "")
    if not expected or token != expected:
        raise AuthError("Invalid read token")
    return True


def validate_admin_token(token: str) -> bool:
    """Validate an admin-level bearer token (for swap operations)."""
    expected = os.environ.get("DATABASE_ADMIN_TOKEN", "")
    if not expected or token != expected:
        raise AuthError("Invalid admin token")
    return True


def get_bearer_token(request: Request) -> str:
    """Extract bearer token from Authorization header."""
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")
    return auth[7:]
