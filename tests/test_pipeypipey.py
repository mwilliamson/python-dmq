import collections
import dataclasses
from typing import List, Optional

from precisely import assert_that, contains_exactly, has_attrs
import pytest
from sqlalchemy import Column, create_engine, ForeignKey, Integer, select, String
from sqlalchemy.orm import declarative_base, relationship, Session

### pipepipey


def pipe(from_type, to_type, *, parent_type = None):
    def f(cls):
        cls.from_type = from_type
        cls.to_type = to_type
        cls.parent_type = parent_type

        return cls

    return f


def entity(cls):
    cls = dataclasses.dataclass(frozen=True)(cls)

    for field in dataclasses.fields(cls):
        setattr(cls, field.name, field)

    return cls


def field(query):
    return dataclasses.field(metadata={"query": query})


class Executor:
    def __init__(self, *, pipes):
        self._pipes = pipes

    def fetch(self, query, *, parent_type = None, parents = None):
        for pipe in self._pipes:
            if isinstance(query, pipe.from_type) and pipe.parent_type == parent_type:
                if parents is None:
                    return pipe(self, query)
                else:
                    return pipe(self, query, parents=parents)

        raise ValueError(f"could not fetch {query}")


### SQLAlchemy


Base = declarative_base()


### Domain


@entity
class Post:
    id: int
    title: str
    body: str

    @classmethod
    def query(cls):
        return PostQuery(cls, title=None)


@dataclasses.dataclass(frozen=True)
class PostQuery:
    cls: type
    title: Optional[str]

    def has_title(self, title: str):
        return dataclasses.replace(self, title=title)


@entity
class Comment:
    id: int
    author: str
    body: str

    @staticmethod
    def query_for_post():
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
    post = relationship(PostModel)


### Plumbing


@pipe(PostQuery, Post)
class PostQueryToPostPipe:
    parent_type = None

    def __init__(self, *, session):
        self._session = session

    def __call__(self, executor, query):
        sql_query = select(PostModel)

        if query.title is not None:
            sql_query = sql_query.filter(PostModel.title == query.title)

        post_models = self._session.execute(sql_query).scalars().all()

        post_dicts = {
            post_model.id: dict(id=post_model.id, title=post_model.title, body=post_model.body)
            for post_model in post_models
        }

        for field in dataclasses.fields(query.cls):
            field_query = field.metadata.get("query")
            if field_query is not None:
                field_values = executor.fetch(field_query, parent_type=Post, parents=post_dicts.values())
                for post_id, field_value in field_values:
                    post_dicts[post_id][field.name] = field_value

        return [
            query.cls(**post_dict)
            for post_dict in post_dicts.values()
        ]


@pipe(CommentQuery, Comment, parent_type=Post)
class PostCommentQueryToCommentPipe:
    def __init__(self, *, session):
        self._session = session

    def __call__(self, executor, query, *, parents):
        sql_query = select(CommentModel).where(CommentModel.post_id.in_([parent["id"] for parent in parents]))

        comment_models = self._session.execute(sql_query).scalars().all()

        result = collections.defaultdict(list)
        for comment_model in comment_models:
            comment = Comment(id=comment_model.id, author=comment_model.author, body=comment_model.body)
            result[comment_model.post_id].append(comment)

        return result.items()


### Tests


def test_can_fetch_all_posts(session):
    post_model_1 = PostModel(title="<post 1>", body="")
    post_model_2 = PostModel(title="<post 2>", body="")
    session.add_all([post_model_1, post_model_2])
    session.commit()

    executor = Executor(pipes=[PostQueryToPostPipe(session=session)])

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


def test_can_filter_posts(session):
    post_model_1 = PostModel(title="<post 1>", body="")
    post_model_2 = PostModel(title="<post 2>", body="")
    session.add_all([post_model_1, post_model_2])
    session.commit()

    executor = Executor(pipes=[PostQueryToPostPipe(session=session)])

    post_query = Post.query().has_title("<post 1>")
    results = executor.fetch(post_query)

    assert_that(results, contains_exactly(
        has_attrs(
            title="<post 1>",
        ),
    ))


def test_can_add_fields_to_objects(session):
    post_model_1 = PostModel(title="<post 1>", body="")
    comment_model_1a = CommentModel(body="<comment 1a>", post=post_model_1, author="<author>")
    comment_model_1b = CommentModel(body="<comment 1b>", post=post_model_1, author="<author>")
    post_model_2 = PostModel(title="<post 2>", body="")
    comment_model_2a = CommentModel(body="<comment 2a>", post=post_model_2, author="<author>")
    session.add_all([post_model_1, comment_model_1a, comment_model_1b, post_model_2, comment_model_2a])
    session.commit()

    executor = Executor(
        pipes=[
            PostQueryToPostPipe(session=session),
            PostCommentQueryToCommentPipe(session=session),
        ],
    )

    @entity
    class PostWithComments(Post):
        comments: List[Comment] = field(Comment.query_for_post())

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
