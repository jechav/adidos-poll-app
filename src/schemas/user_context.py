"""Identity resolved from an authenticated request (I-003/I-004)."""

from typing import Literal

from pydantic import BaseModel


class UserContext(BaseModel):
    """Resolved identity for the caller of an authenticated route (I-004)."""

    user_id: str
    role: Literal["user", "admin"] = "user"
