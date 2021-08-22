from __future__ import annotations

import dataclasses
from datetime import datetime, timedelta
from typing import List, Optional, Type, TypeVar

import dmq


T = TypeVar("T")


@dataclasses.dataclass(frozen=True)
class Post:
    id: int
    title: str
    body: str

    @classmethod
    def query(cls: Type[T]) -> PostQuery[T]:
        return PostQuery(cls, title=None)


@dataclasses.dataclass(frozen=True)
class PostQuery(dmq.Query[T]):
    result_type: Type[T]
    title: Optional[str]

    def has_title(self, title: str) -> PostQuery[T]:
        return dataclasses.replace(self, title=title)


@dataclasses.dataclass(frozen=True)
class Comment:
    id: int
    author_id: int
    created_at: datetime
    body: str

    @classmethod
    def query(cls: Type[T]) -> CommentQuery:
        return CommentQuery(cls, created_in_last=None)


@dataclasses.dataclass(frozen=True)
class CommentQuery(dmq.Query[T]):
    result_type: Type[T]
    created_in_last: Optional[timedelta]

    def recent(self) -> CommentQuery:
        return dataclasses.replace(self, created_in_last=timedelta(days=1))


@dataclasses.dataclass(frozen=True)
class User:
    username: str

    @classmethod
    def query(cls) -> UserQuery:
        return UserQuery(cls)


@dataclasses.dataclass(frozen=True)
class UserQuery(dmq.Query[T]):
    result_type: Type[T]
