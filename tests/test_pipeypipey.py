import dataclasses
from typing import List, Optional

from precisely import assert_that, contains_exactly, has_attrs
import pytest
from sqlalchemy import Column, create_engine, Integer, select, String
from sqlalchemy.orm import declarative_base, Session

### pipepipey

def source(of_type):
    def f(cls):
        cls.of_type = of_type

        return cls

    return f


def pipe(from_type, to_type):
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


class Executor:
    def __init__(self, *, pipes):
        self._pipes = pipes

    def fetch(self, query):
        for pipe in self._pipes:
            if isinstance(query, pipe.from_type):
                return pipe(query)


### SQLAlchemy


Base = declarative_base()


### Domain


@entity
class Post:
    id: int
    title: str
    body: str

    @staticmethod
    def query():
        return PostQuery(title=None)


@dataclasses.dataclass
class PostQuery:
    title: Optional[str]

    def has_title(self, title: str):
        return dataclasses.replace(self, title=title)


### SQL Models


class PostModel(Base):
    __tablename__ = "post"

    id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    body = Column(String, nullable=False)


### Plumbing


@pipe(PostQuery, Post)
class PostQueryToPostPipe:
    def __init__(self, *, session):
        self._session = session

    def __call__(self, query):
        sql_query = select(PostModel)

        if query.title is not None:
            sql_query = sql_query.filter(PostModel.title == query.title)

        return self._session.execute(sql_query).scalars().all()


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


@pytest.fixture(name="session")
def _fixture_session():
    engine = create_engine("sqlite+pysqlite:///:memory:", echo=False, future=True)
    Base.metadata.create_all(engine)
    return Session(engine)
