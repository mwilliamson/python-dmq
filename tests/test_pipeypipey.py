from __future__ import annotations

import collections
import dataclasses
from typing import Any, Dict, Generic, List, Optional, Type, TypeVar

from precisely import assert_that, contains_exactly, has_attrs
import pytest
from sqlalchemy import Column, create_engine, ForeignKey, Integer, select, String
from sqlalchemy.orm import declarative_base, relationship, Session

### pipepipey


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
    pass


class Executor:
    def __init__(self, *, root_fetchers, field_fetchers):
        self._root_fetchers = root_fetchers
        self._field_fetchers = field_fetchers

    def fetch(self, query: Query[T]) -> List[T]:
        for fetcher in self._root_fetchers:
            if isinstance(query, fetcher.from_type):
                return fetcher(self, query)

        raise ValueError(f"could not fetch {query}")

    def fetch_field(self, query, *, parent_type, parents):
        for fetcher in self._field_fetchers:
            if isinstance(query, fetcher.from_type) and fetcher.parent_type == parent_type:
                return fetcher(self, query, parents=parents)

        raise ValueError(f"could not fetch {query}")


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
    author: str
    body: str

    @staticmethod
    def query() -> CommentQuery:
        return CommentQuery()


@dataclasses.dataclass(frozen=True)
class CommentQuery:
    pass

### SQL Models


class PostModel(Base):
    __tablename__ = "post"

    id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    body = Column(String, nullable=False)


class CommentModel(Base):
    __tablename__ = "comment"

    id = Column(Integer, primary_key=True)
    author = Column(String, nullable=False)
    body = Column(String, nullable=False)
    post_id = Column(Integer, ForeignKey("post.id"), nullable=False)
    post = relationship(PostModel, uselist=False)


### Plumbing


@root_fetcher(PostQuery, Post)
class PostFetcher:
    def __init__(self, *, session: Session):
        self._session = session

    def __call__(self, executor: Executor, query: PostQuery[T]) -> List[T]:
        sql_query = select(PostModel)

        if query.title is not None:
            sql_query = sql_query.filter(PostModel.title == query.title)

        post_models = self._session.execute(sql_query).scalars().all()

        posts = [Post(id=post_model.id, title=post_model.title, body=post_model.body) for post_model in post_models]

        extra_field_values: List[Dict[str, Any]] = [{} for _ in posts]

        for field in dataclasses.fields(query.result_type):
            field_query = field.metadata.get("query")
            if field_query is not None:
                field_values = executor.fetch_field(field_query, parent_type=Post, parents=posts)
                for field_values, field_value in zip(extra_field_values, field_values):
                    field_values[field.name] = field_value

        return [
            query.result_type(**dataclasses.asdict(post), **field_values)  # type: ignore
            for post, field_values in zip(posts, extra_field_values)
        ]


@field_fetcher(CommentQuery, Comment, parent_type=Post)
class PostCommentFetcher:
    def __init__(self, *, session: Session):
        self._session = session

    def __call__(self, executor: Executor, query: CommentQuery, *, parents: List[Post]):
        sql_query = select(CommentModel).where(CommentModel.post_id.in_([parent.id for parent in parents]))

        comment_models = self._session.execute(sql_query).scalars().all()

        result = collections.defaultdict(list)
        for comment_model in comment_models:
            comment = Comment(id=comment_model.id, author=comment_model.author, body=comment_model.body)
            result[comment_model.post_id].append(comment)

        return [result[parent.id] for parent in parents]


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
    post_model_1 = PostModel(title="<post 1>", body="")
    comment_model_1a = CommentModel(body="<comment 1a>", post=post_model_1, author="<author>")
    comment_model_1b = CommentModel(body="<comment 1b>", post=post_model_1, author="<author>")
    post_model_2 = PostModel(title="<post 2>", body="")
    comment_model_2a = CommentModel(body="<comment 2a>", post=post_model_2, author="<author>")
    session.add_all([post_model_1, comment_model_1a, comment_model_1b, post_model_2, comment_model_2a])
    session.commit()

    executor = Executor(
        root_fetchers=[PostFetcher(session=session)],
        field_fetchers=[PostCommentFetcher(session=session)],
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


@pytest.fixture(name="session")
def _fixture_session():
    engine = create_engine("sqlite+pysqlite:///:memory:", echo=False, future=True)
    Base.metadata.create_all(engine)
    return Session(engine)
