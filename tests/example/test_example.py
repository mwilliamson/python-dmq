from datetime import datetime
from typing import List

from precisely import assert_that, contains_exactly, has_attrs
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

import dmq
from . import database
from .database_adapters import CommentAuthorFetcher, PostFetcher, PostCommentFetcher
from .domain import Comment, User, Post


def test_can_fetch_all_posts(session: Session) -> None:
    post_model_1 = database.Post(title="<post 1>", body="")
    post_model_2 = database.Post(title="<post 2>", body="")
    session.add_all([post_model_1, post_model_2])
    session.commit()

    executor = dmq.Executor(
        root_fetchers=[PostFetcher(session=session)],
        field_fetchers=[],
    )

    results = executor.fetch(Post.query())

    assert_that(results, contains_exactly(
        has_attrs(
            id=post_model_1.id,
            title="<post 1>",
        ),
        has_attrs(
            id=post_model_2.id,
            title="<post 2>",
        ),
    ))


def test_can_filter_posts(session: Session) -> None:
    post_model_1 = database.Post(title="<post 1>", body="")
    post_model_2 = database.Post(title="<post 2>", body="")
    session.add_all([post_model_1, post_model_2])
    session.commit()

    executor = dmq.Executor(
        root_fetchers=[PostFetcher(session=session)],
        field_fetchers=[],
    )

    post_query = Post.query().has_title("<post 1>")
    results = executor.fetch(post_query)

    assert_that(results, contains_exactly(
        has_attrs(
            title="<post 1>",
        ),
    ))


def test_can_add_fields_to_objects(session: Session) -> None:
    author = database.User(username="<author>")
    post_model_1 = database.Post(title="<post 1>", body="")
    comment_model_1a = database.Comment(body="<comment 1a>", post=post_model_1, author=author, created_at=datetime(2000, 1, 1))
    comment_model_1b = database.Comment(body="<comment 1b>", post=post_model_1, author=author, created_at=datetime(2000, 1, 1))
    post_model_2 = database.Post(title="<post 2>", body="")
    comment_model_2a = database.Comment(body="<comment 2a>", post=post_model_2, author=author, created_at=datetime(2000, 1, 1))
    session.add_all([post_model_1, comment_model_1a, comment_model_1b, post_model_2, comment_model_2a])
    session.commit()

    executor = dmq.Executor(
        root_fetchers=[PostFetcher(session=session)],
        field_fetchers=[PostCommentFetcher(session=session, now=datetime(2020, 1, 1, 12))],
    )

    @dmq.entity
    class PostWithComments(Post):
        comments: List[Comment] = dmq.field(Comment.query())

    post_query = PostWithComments.query()
    results = executor.fetch(post_query)

    assert_that(results, contains_exactly(
        has_attrs(
            title="<post 1>",
            comments=contains_exactly(
                has_attrs(body="<comment 1a>"),
                has_attrs(body="<comment 1b>"),
            ),
        ),
        has_attrs(
            title="<post 2>",
            comments=contains_exactly(
                has_attrs(body="<comment 2a>"),
            ),
        ),
    ))


def test_can_filter_fields_added_to_objects(session: Session) -> None:
    author = database.User(username="<author>")
    post_model_1 = database.Post(title="<post 1>", body="")
    comment_model_1a = database.Comment(body="<comment 1a>", post=post_model_1, author=author, created_at=datetime(2000, 1, 1))
    comment_model_1b = database.Comment(body="<comment 1b>", post=post_model_1, author=author, created_at=datetime(2021, 1, 1))
    post_model_2 = database.Post(title="<post 2>", body="")
    comment_model_2a = database.Comment(body="<comment 2a>", post=post_model_2, author=author, created_at=datetime(2000, 1, 1))
    session.add_all([post_model_1, comment_model_1a, comment_model_1b, post_model_2, comment_model_2a])
    session.commit()

    executor = dmq.Executor(
        root_fetchers=[PostFetcher(session=session)],
        field_fetchers=[PostCommentFetcher(session=session, now=datetime(2021, 1, 1, 12))],
    )

    @dmq.entity
    class PostWithRecentComments(Post):
        recent_comments: List[Comment] = dmq.field(Comment.query().recent())

    post_query = PostWithRecentComments.query()
    results = executor.fetch(post_query)

    assert_that(results, contains_exactly(
        has_attrs(
            title="<post 1>",
            recent_comments=contains_exactly(
                has_attrs(body="<comment 1b>"),
            ),
        ),
        has_attrs(
            title="<post 2>",
            recent_comments=contains_exactly(),
        ),
    ))


def test_can_add_subfields_to_fields(session: Session) -> None:
    author_1 = database.User(username="<author 1>")
    author_2 = database.User(username="<author 2>")
    post_model_1 = database.Post(title="<post 1>", body="")
    comment_model_1a = database.Comment(body="<comment 1a>", post=post_model_1, author=author_1, created_at=datetime(2000, 1, 1))
    comment_model_1b = database.Comment(body="<comment 1b>", post=post_model_1, author=author_2, created_at=datetime(2021, 1, 1))
    post_model_2 = database.Post(title="<post 2>", body="")
    comment_model_2a = database.Comment(body="<comment 2a>", post=post_model_2, author=author_1, created_at=datetime(2000, 1, 1))
    session.add_all([post_model_1, comment_model_1a, comment_model_1b, post_model_2, comment_model_2a])
    session.commit()

    executor = dmq.Executor(
        root_fetchers=[PostFetcher(session=session)],
        field_fetchers=[
            PostCommentFetcher(session=session, now=datetime(2021, 1, 1, 12)),
            CommentAuthorFetcher(session=session),
        ],
    )

    @dmq.entity
    class CommentWithAuthor(Comment):
        author: User = dmq.field(User.query())

    @dmq.entity
    class PostWithComments(Post):
        comments: List[CommentWithAuthor] = dmq.field(CommentWithAuthor.query())

    post_query = PostWithComments.query()
    results = executor.fetch(post_query)

    assert_that(results, contains_exactly(
        has_attrs(
            title="<post 1>",
            comments=contains_exactly(
                has_attrs(
                    author=has_attrs(username="<author 1>"),
                    body="<comment 1a>",
                ),
                has_attrs(
                    author=has_attrs(username="<author 2>"),
                    body="<comment 1b>",
                ),
            ),
        ),
        has_attrs(
            title="<post 2>",
            comments=contains_exactly(
                has_attrs(
                    author=has_attrs(username="<author 1>"),
                    body="<comment 2a>",
                ),
            ),
        ),
    ))


@pytest.fixture(name="session")
def _fixture_session():
    engine = create_engine("sqlite+pysqlite:///:memory:", echo=False, future=True)
    database.create_schema(engine)
    return Session(engine)
