
from sqlalchemy import Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.engine import Engine
from sqlalchemy.orm import declarative_base, relationship


Base = declarative_base()


class User(Base):
    __tablename__ = "user"

    id = Column(Integer, primary_key=True)
    username = Column(String, nullable=False)


class Post(Base):
    __tablename__ = "post"

    id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    body = Column(String, nullable=False)


class Comment(Base):
    __tablename__ = "comment"

    id = Column(Integer, primary_key=True)
    author_id = Column(Integer, ForeignKey("user.id"), nullable=False)
    author = relationship(User, uselist=False)
    created_at = Column(DateTime, nullable=False)
    body = Column(String, nullable=False)
    post_id = Column(Integer, ForeignKey("post.id"), nullable=False)
    post = relationship(Post, uselist=False)


def create_schema(engine: Engine) -> None:
    Base.metadata.create_all(engine)
