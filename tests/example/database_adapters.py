import collections
from datetime import datetime
from typing import List, TypeVar

from sqlalchemy import select
from sqlalchemy.orm import Session

import dmq
from . import database
from .domain import Comment, CommentQuery, Post, PostQuery, User, UserQuery


TPost = TypeVar("TPost", bound=Post)


@dmq.root_fetcher(PostQuery, Post)
class PostFetcher:
    def __init__(self, *, session: Session):
        self._session = session

    def __call__(self, executor: dmq.Executor, query: PostQuery[TPost]) -> List[Post]:
        sql_query = select(database.Post)

        if query.title is not None:
            sql_query = sql_query.filter(database.Post.title == query.title)

        post_rows = self._session.execute(sql_query).scalars().all()

        return [Post(id=post_model.id, title=post_model.title, body=post_model.body) for post_model in post_rows]


@dmq.field_fetcher(CommentQuery, Comment, parent_type=Post)
class PostCommentFetcher:
    def __init__(self, *, now: datetime, session: Session):
        self._now = now
        self._session = session

    def __call__(self, executor: dmq.Executor, query: CommentQuery, *, parents: List[Post]):
        sql_query = select(database.Comment).where(database.Comment.post_id.in_([parent.id for parent in parents]))

        if query.created_in_last is not None:
            sql_query = sql_query.filter(database.Comment.created_at >= (self._now - query.created_in_last))

        comment_rows = self._session.execute(sql_query).scalars().all()

        result = collections.defaultdict(list)
        for comment_model in comment_rows:
            comment = Comment(id=comment_model.id, author_id=comment_model.author_id, body=comment_model.body, created_at=comment_model.created_at)
            result[comment_model.post_id].append(comment)

        return [result[parent.id] for parent in parents]


@dmq.field_fetcher(UserQuery, User, parent_type=Comment)
class CommentAuthorFetcher:
    def __init__(self, *, session: Session):
        self._session = session

    def __call__(self, executor: dmq.Executor, query: UserQuery, *, parents: List[Comment]):
        sql_query = select(database.User).where(database.User.id.in_([parent.author_id for parent in parents]))

        user_rows = self._session.execute(sql_query).scalars().all()

        result = {
            user_model.id: User(username=user_model.username)
            for user_model in user_rows
        }

        return [result[parent.author_id] for parent in parents]
