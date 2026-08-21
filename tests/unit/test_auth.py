import pytest
from fastapi import HTTPException

from src.api.dependencies.auth import extract_bearer_token, get_current_user, require_admin


def test_extract_bearer_token_missing_header():
    with pytest.raises(HTTPException) as exc_info:
        extract_bearer_token(None)
    assert exc_info.value.status_code == 401
    assert exc_info.value.detail["code"] == "UNAUTHORIZED"


def test_extract_bearer_token_malformed_header():
    with pytest.raises(HTTPException) as exc_info:
        extract_bearer_token("Token abc123")
    assert exc_info.value.status_code == 401


def test_extract_bearer_token_valid_header():
    assert extract_bearer_token("Bearer abc123") == "abc123"


@pytest.mark.asyncio
async def test_get_current_user_resolves_regular_user():
    user = await get_current_user(token="user-token")
    assert user.user_id == "user-token"
    assert user.role == "user"


@pytest.mark.asyncio
async def test_get_current_user_resolves_admin():
    user = await get_current_user(token="admin:ops-1")
    assert user.user_id == "ops-1"
    assert user.role == "admin"


@pytest.mark.asyncio
async def test_require_admin_accepts_admin():
    user = await get_current_user(token="admin:ops-1")
    result = await require_admin(user=user)
    assert result.role == "admin"


@pytest.mark.asyncio
async def test_require_admin_rejects_regular_user():
    user = await get_current_user(token="user-token")
    with pytest.raises(HTTPException) as exc_info:
        await require_admin(user=user)
    assert exc_info.value.status_code == 403
    assert exc_info.value.detail["code"] == "FORBIDDEN"
