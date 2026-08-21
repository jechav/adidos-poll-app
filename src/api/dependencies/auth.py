"""Token extraction and identity resolution for route handlers (I-003).

`extract_bearer_token` and the 401/403 error shape here are the contract
I-004 builds on. I-004 owns the real strategy: hashing the token, checking
the `auth:token:{token_hash}` Redis cache (I-002), and decoding an
Adidos-issued token on a cache miss ("trust-and-cache", spec decision #1).
Until that lands, `get_current_user` resolves identity directly from the
bearer token with no cache and no real Adidos token format, so routes have
something to `Depends()` on: the token string itself is used as `user_id`,
and a `admin:` prefix is treated as the admin role.
"""

from fastapi import Depends, Header, HTTPException

from src.schemas.user_context import UserContext


def _unauthorized() -> HTTPException:
    return HTTPException(
        status_code=401,
        detail={
            "code": "UNAUTHORIZED",
            "message": "Missing or malformed Authorization header",
        },
    )


def extract_bearer_token(authorization: str | None = Header(default=None)) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise _unauthorized()
    token = authorization.removeprefix("Bearer ").strip()
    if not token:
        raise _unauthorized()
    return token


async def get_current_user(token: str = Depends(extract_bearer_token)) -> UserContext:
    if token.startswith("admin:"):
        return UserContext(user_id=token.removeprefix("admin:"), role="admin")
    return UserContext(user_id=token, role="user")


async def require_admin(user: UserContext = Depends(get_current_user)) -> UserContext:
    if user.role != "admin":
        raise HTTPException(
            status_code=403,
            detail={"code": "FORBIDDEN", "message": "User is not admin"},
        )
    return user
