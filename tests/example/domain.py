from __future__ import annotations

import dataclasses
from datetime import datetime, timedelta
from typing import Generic, Optional, Type, TypeVar


TPost = TypeVar("TPost", bound="Post")


@dataclasses.dataclass(frozen=True)
class Post:
    id: int
    title: str
    body: str

    @classmethod
    def query(cls: Type[TPost]) -> PostQuery[TPost]:
        return PostQuery(cls, title=None)


@dataclasses.dataclass(frozen=True)
class PostQuery(Generic[TPost]):
    element_type: Type[TPost]
    title: Optional[str]

    def has_title(self, title: str) -> PostQuery[TPost]:
        return dataclasses.replace(self, title=title)


TComment = TypeVar("TComment", bound="Comment")


@dataclasses.dataclass(frozen=True)
class Comment:
    id: int
    author_id: int
    created_at: datetime
    body: str

    @classmethod
    def query(cls: Type[TComment]) -> CommentQuery[TComment]:
        return CommentQuery(cls, created_in_last=None)


@dataclasses.dataclass(frozen=True)
class CommentQuery(Generic[TComment]):
    element_type: Type[TComment]
    created_in_last: Optional[timedelta]

    def recent(self) -> CommentQuery:
        return dataclasses.replace(self, created_in_last=timedelta(days=1))


TUser = TypeVar("TUser", bound="User")


@dataclasses.dataclass(frozen=True)
class User:
    username: str

    @classmethod
    def query(cls) -> UserQuery:
        return UserQuery(cls)


@dataclasses.dataclass(frozen=True)
class UserQuery(Generic[TUser]):
    element_type: Type[TUser]
