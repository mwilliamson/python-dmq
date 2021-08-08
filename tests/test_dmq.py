from __future__ import annotations

import collections
import dataclasses
from datetime import datetime, timedelta
from typing import Any, Dict, Generic, List, Optional, Type, TypeVar

from precisely import assert_that, contains_exactly, has_attrs
import pytest
from sqlalchemy import Column, create_engine, DateTime, ForeignKey, Integer, select, String
from sqlalchemy.orm import declarative_base, relationship, Session

### dmq


def field_fetcher(from_type, to_type, *, parent_type = None):
    def f(cls):
        cls.from_type = from_type
        cls.to_type = to_type
        cls.parent_type = parent_type

        return cls

    return f


def root_fetcher(from_type, to_type):
    def f(cls):
        cls.from_type = from_type
        cls.to_type = to_type

        return cls

    return f


def entity(cls):
    cls = dataclasses.dataclass(frozen=True)(cls)

    for field in dataclasses.fields(cls):
        setattr(cls, field.name, field)

    return cls


def field(query):
    return dataclasses.field(metadata={"query": query})


T = TypeVar("T")


class Query(Generic[T]):
    result_type: Type[T]


class Executor:
    def __init__(self, *, root_fetchers, field_fetchers):
        self._root_fetchers = root_fetchers
        self._field_fetchers = field_fetchers

    def fetch(self, query: Query[T]) -> List[T]:
        core_type, cores = self._fetch_core(query)
        return self._add_fields(cores, query, parent_type=core_type)

    def _fetch_core(self, query: Query[T]) -> List[T]:
        for fetcher in self._root_fetchers:
            if isinstance(query, fetcher.from_type):
                return fetcher.to_type, fetcher(self, query)

        raise ValueError(f"could not fetch {query}")

    def _add_fields(self, cores: List[T], query: Query[T], *, parent_type) -> List[T]:
        extra_field_values: List[Dict[str, Any]] = [{} for _ in cores]

        for field in dataclasses.fields(query.result_type):
            field_query = field.metadata.get("query")
            if field_query is not None:
                field_values = self._fetch_field(field_query, parent_type=parent_type, parents=cores)
                for field_values, field_value in zip(extra_field_values, field_values):
                    field_values[field.name] = field_value

        return [
            query.result_type(**dataclasses.asdict(core), **field_values)  # type: ignore
            for core, field_values in zip(cores, extra_field_values)
        ]

    def _fetch_field(self, query, *, parent_type, parents):
        core_type, cores = self._fetch_field_core(query, parent_type=parent_type, parents=parents)

        def flatten(cores):
            result = []

            def f(value):
                if isinstance(value, list):
                    for element in value:
                        f(element)
                else:
                    result.append(value)

            f(cores)

            return result

        def unflatten(results):
            results_iter = iter(results)

            def f(value):
                if isinstance(value, list):
                    return [f(element) for element in value]
                else:
                    return next(results_iter)

            return f(cores)

        return unflatten(self._add_fields(flatten(cores), query, parent_type=core_type))

    def _fetch_field_core(self, query, *, parent_type, parents):
        for fetcher in self._field_fetchers:
            if isinstance(query, fetcher.from_type) and fetcher.parent_type == parent_type:
                return fetcher.to_type, fetcher(self, query, parents=parents)

        raise ValueError(f"could not fetch {query} for field on {parent_type}")


### SQLAlchemy


Base = declarative_base()


### Domain


@dataclasses.dataclass(frozen=True)
class Post:
    id: int
    title: str
    body: str

    @classmethod
    def query(cls: Type[T]) -> PostQuery[T]:
        return PostQuery(cls, title=None)


@dataclasses.dataclass(frozen=True)
class PostQuery(Query[T]):
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
class CommentQuery(Query[T]):
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
class UserQuery(Query[T]):
    result_type: Type[T]


### SQL Models


class PostModel(Base):
    __tablename__ = "post"

    id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    body = Column(String, nullable=False)


class CommentModel(Base):
    __tablename__ = "comment"

    id = Column(Integer, primary_key=True)
    author_id = Column(Integer, ForeignKey("user.id"), nullable=False)
    author = relationship(lambda: UserModel, uselist=False)
    created_at = Column(DateTime, nullable=False)
    body = Column(String, nullable=False)
    post_id = Column(Integer, ForeignKey("post.id"), nullable=False)
    post = relationship(PostModel, uselist=False)


class UserModel(Base):
    __tablename__ = "user"

    id = Column(Integer, primary_key=True)
    username = Column(String, nullable=False)


### Plumbing


@root_fetcher(PostQuery, Post)
class PostFetcher:
    def __init__(self, *, session: Session):
        self._session = session

    def __call__(self, executor: Executor, query: PostQuery[T]) -> List[Post]:
        sql_query = select(PostModel)

        if query.title is not None:
            sql_query = sql_query.filter(PostModel.title == query.title)

        post_models = self._session.execute(sql_query).scalars().all()

        return [Post(id=post_model.id, title=post_model.title, body=post_model.body) for post_model in post_models]


@field_fetcher(CommentQuery, Comment, parent_type=Post)
class PostCommentFetcher:
    def __init__(self, *, now: datetime, session: Session):
        self._now = now
        self._session = session

    def __call__(self, executor: Executor, query: CommentQuery, *, parents: List[Post]):
        sql_query = select(CommentModel).where(CommentModel.post_id.in_([parent.id for parent in parents]))

        if query.created_in_last is not None:
            sql_query = sql_query.filter(CommentModel.created_at >= (self._now - query.created_in_last))

        comment_models = self._session.execute(sql_query).scalars().all()

        result = collections.defaultdict(list)
        for comment_model in comment_models:
            comment = Comment(id=comment_model.id, author_id=comment_model.author_id, body=comment_model.body, created_at=comment_model.created_at)
            result[comment_model.post_id].append(comment)

        return [result[parent.id] for parent in parents]


@field_fetcher(UserQuery, User, parent_type=Comment)
class CommentAuthorFetcher:
    def __init__(self, *, session: Session):
        self._session = session

    def __call__(self, executor: Executor, query: UserQuery, *, parents: List[Comment]):
        sql_query = select(UserModel).where(UserModel.id.in_([parent.author_id for parent in parents]))

        user_models = self._session.execute(sql_query).scalars().all()

        result = {
            user_model.id: User(username=user_model.username)
            for user_model in user_models
        }

        return [result[parent.author_id] for parent in parents]


### Tests


def test_can_fetch_all_posts(session: Session) -> None:
    post_model_1 = PostModel(title="<post 1>", body="")
    post_model_2 = PostModel(title="<post 2>", body="")
    session.add_all([post_model_1, post_model_2])
    session.commit()

    executor = Executor(
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
    post_model_1 = PostModel(title="<post 1>", body="")
    post_model_2 = PostModel(title="<post 2>", body="")
    session.add_all([post_model_1, post_model_2])
    session.commit()

    executor = Executor(
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
    author = UserModel(username="<author>")
    post_model_1 = PostModel(title="<post 1>", body="")
    comment_model_1a = CommentModel(body="<comment 1a>", post=post_model_1, author=author, created_at=datetime(2000, 1, 1))
    comment_model_1b = CommentModel(body="<comment 1b>", post=post_model_1, author=author, created_at=datetime(2000, 1, 1))
    post_model_2 = PostModel(title="<post 2>", body="")
    comment_model_2a = CommentModel(body="<comment 2a>", post=post_model_2, author=author, created_at=datetime(2000, 1, 1))
    session.add_all([post_model_1, comment_model_1a, comment_model_1b, post_model_2, comment_model_2a])
    session.commit()

    executor = Executor(
        root_fetchers=[PostFetcher(session=session)],
        field_fetchers=[PostCommentFetcher(session=session, now=datetime(2020, 1, 1, 12))],
    )

    @entity
    class PostWithComments(Post):
        comments: List[Comment] = field(Comment.query())

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
    author = UserModel(username="<author>")
    post_model_1 = PostModel(title="<post 1>", body="")
    comment_model_1a = CommentModel(body="<comment 1a>", post=post_model_1, author=author, created_at=datetime(2000, 1, 1))
    comment_model_1b = CommentModel(body="<comment 1b>", post=post_model_1, author=author, created_at=datetime(2021, 1, 1))
    post_model_2 = PostModel(title="<post 2>", body="")
    comment_model_2a = CommentModel(body="<comment 2a>", post=post_model_2, author=author, created_at=datetime(2000, 1, 1))
    session.add_all([post_model_1, comment_model_1a, comment_model_1b, post_model_2, comment_model_2a])
    session.commit()

    executor = Executor(
        root_fetchers=[PostFetcher(session=session)],
        field_fetchers=[PostCommentFetcher(session=session, now=datetime(2021, 1, 1, 12))],
    )

    @entity
    class PostWithRecentComments(Post):
        recent_comments: List[Comment] = field(Comment.query().recent())

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
    author_1 = UserModel(username="<author 1>")
    author_2 = UserModel(username="<author 2>")
    post_model_1 = PostModel(title="<post 1>", body="")
    comment_model_1a = CommentModel(body="<comment 1a>", post=post_model_1, author=author_1, created_at=datetime(2000, 1, 1))
    comment_model_1b = CommentModel(body="<comment 1b>", post=post_model_1, author=author_2, created_at=datetime(2021, 1, 1))
    post_model_2 = PostModel(title="<post 2>", body="")
    comment_model_2a = CommentModel(body="<comment 2a>", post=post_model_2, author=author_1, created_at=datetime(2000, 1, 1))
    session.add_all([post_model_1, comment_model_1a, comment_model_1b, post_model_2, comment_model_2a])
    session.commit()

    executor = Executor(
        root_fetchers=[PostFetcher(session=session)],
        field_fetchers=[
            PostCommentFetcher(session=session, now=datetime(2021, 1, 1, 12)),
            CommentAuthorFetcher(session=session),
        ],
    )

    @entity
    class CommentWithAuthor(Comment):
        author: User = field(User.query())

    @entity
    class PostWithComments(Post):
        comments: List[CommentWithAuthor] = field(CommentWithAuthor.query())

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
    Base.metadata.create_all(engine)
    return Session(engine)
