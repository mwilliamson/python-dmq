import dataclasses
from typing import List

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


@dataclasses.dataclass(frozen=True)
class Query:
    of_type: type
    filters: List[object]

    def where(self, filter: object):
        return dataclasses.replace(self, filters=self.filters + (filter, ))


def entity(cls):
    cls = dataclasses.dataclass(frozen=True)(cls)

    for field in dataclasses.fields(cls):
        setattr(cls, field.name, field)

    return cls


@dataclasses.dataclass(frozen=True)
class FilterEqual:
    left: object
    right: object


def eq(left, right):
    return FilterEqual(left, right)


def query(of_type):
    return Query(of_type=of_type, filters=())


class Executor:
    def __init__(self, *, sources, pipes):
        self._sources = sources
        self._pipes = pipes

    def fetch(self, query):
        for source in self._sources:
            if query.of_type == source.of_type:
                return source(query)


### SQLAlchemy


Base = declarative_base()


### Domain


@entity
class Post:
    id: int
    title: str
    body: str


### Models


class PostModel(Base):
    __tablename__ = "post"

    id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    body = Column(String, nullable=False)


### Plumbing


@source(Post)
class PostSqlSource:
    def __init__(self, *, session):
        self._session = session

    def __call__(self, query):
        sql_query = select(PostModel)

        for filter in query.filters:
            assert isinstance(filter, FilterEqual)
            fields_to_sql_expression = {
                Post.title: PostModel.title
            }
            sql_query = sql_query.filter(fields_to_sql_expression[filter.left] == filter.right)

        return self._session.execute(sql_query).scalars().all()


### Tests


def test_can_fetch_all_posts(session):
    post_model_1 = PostModel(title="<post 1>", body="")
    post_model_2 = PostModel(title="<post 2>", body="")
    session.add_all([post_model_1, post_model_2])
    session.commit()

    executor = Executor(sources=[PostSqlSource(session=session)], pipes=[])

    results = executor.fetch(query(Post))

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

    executor = Executor(
        sources=[PostSqlSource(session=session)],
        pipes=[],
    )

    post_query = query(Post).where(eq(Post.title, "<post 1>"))
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
