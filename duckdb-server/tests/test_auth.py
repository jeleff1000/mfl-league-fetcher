"""Tests for auth module."""

import pytest
from auth import validate_read_token, validate_admin_token, AuthError


def test_valid_read_token(monkeypatch):
    monkeypatch.setenv("DATABASE_READ_TOKEN", "read-secret")
    assert validate_read_token("read-secret") is True


def test_invalid_read_token(monkeypatch):
    monkeypatch.setenv("DATABASE_READ_TOKEN", "read-secret")
    with pytest.raises(AuthError):
        validate_read_token("wrong")


def test_missing_read_token_env(monkeypatch):
    monkeypatch.delenv("DATABASE_READ_TOKEN", raising=False)
    with pytest.raises(AuthError):
        validate_read_token("anything")


def test_valid_admin_token(monkeypatch):
    monkeypatch.setenv("DATABASE_ADMIN_TOKEN", "admin-secret")
    assert validate_admin_token("admin-secret") is True


def test_invalid_admin_token(monkeypatch):
    monkeypatch.setenv("DATABASE_ADMIN_TOKEN", "admin-secret")
    with pytest.raises(AuthError):
        validate_admin_token("wrong")


def test_admin_token_rejected_on_read(monkeypatch):
    monkeypatch.setenv("DATABASE_READ_TOKEN", "read-secret")
    monkeypatch.setenv("DATABASE_ADMIN_TOKEN", "admin-secret")
    with pytest.raises(AuthError):
        validate_read_token("admin-secret")
