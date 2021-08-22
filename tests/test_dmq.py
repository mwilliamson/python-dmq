import dataclasses
from typing import Generic, List, Type, TypeVar

from precisely import assert_that, equal_to, has_attrs, is_sequence

import dmq


def test_can_fetch_base_objects_from_root() -> None:
    @dataclasses.dataclass(frozen=True)
    class Post:
        title: str

    @dataclasses.dataclass(frozen=True)
    class PostQuery:
        element_type = Post

    posts = [Post(title="<post 1>"), Post(title="<post 2>")]

    @dmq.root_fetcher(PostQuery, Post)
    class PostFetcher:
        def __call__(self, executor: dmq.Executor, query: PostQuery) -> List[Post]:
            return posts


    executor = dmq.Executor(root_fetchers=[PostFetcher()], field_fetchers=[])
    posts = executor.fetch(PostQuery())

    assert_that(posts, equal_to(posts))


def test_can_fetch_objects_extended_with_scalar_from_root() -> None:
    @dataclasses.dataclass(frozen=True)
    class Post:
        title: str

    TPost = TypeVar("TPost", bound=Post)

    @dataclasses.dataclass(frozen=True)
    class PostQuery(Generic[TPost]):
        element_type: Type[TPost]

    posts = [Post(title="<post 1>"), Post(title="<post 2>")]

    @dmq.root_fetcher(PostQuery, Post)
    class PostFetcher:
        def __call__(self, executor: dmq.Executor, query: PostQuery) -> List[Post]:
            return posts

    class PostUppercaseTitleQuery:
        element_type = str

    @dataclasses.dataclass(frozen=True)
    class PostWithUppercaseTitle(Post):
        uppercase_title: str = dmq.field(query=PostUppercaseTitleQuery())

    @dmq.field_fetcher(PostUppercaseTitleQuery, str, parent_type=Post)
    class PostUppercaseTitleFetcher:
        def __call__(self, executor: dmq.Executor, query: PostQuery, *, parents: List[Post]) -> List[str]:
            return [parent.title.upper() for parent in parents]

    executor = dmq.Executor(root_fetchers=[PostFetcher()], field_fetchers=[PostUppercaseTitleFetcher()])
    posts = executor.fetch(PostQuery(element_type=PostWithUppercaseTitle))

    assert_that(posts, is_sequence(
        has_attrs(title="<post 1>", uppercase_title="<POST 1>"),
        has_attrs(title="<post 2>", uppercase_title="<POST 2>"),
    ))


def test_can_fetch_objects_extended_with_extended_domain_object_from_root() -> None:
    @dataclasses.dataclass(frozen=True)
    class Post:
        title: str

    TPost = TypeVar("TPost", bound=Post)

    @dataclasses.dataclass(frozen=True)
    class PostQuery(Generic[TPost]):
        element_type: Type[TPost]

    @dataclasses.dataclass(frozen=True)
    class Comment:
        body: str

    TComment = TypeVar("TComment", bound=Comment)

    @dataclasses.dataclass(frozen=True)
    class CommentQuery(Generic[TComment]):
        element_type: Type[TComment]

    @dataclasses.dataclass(frozen=True)
    class User:
        name: str

    TUser = TypeVar("TUser", bound=User)

    @dataclasses.dataclass(frozen=True)
    class UserQuery(Generic[TUser]):
        element_type: Type[TUser]

    posts = [Post(title="<post 1>"), Post(title="<post 2>"), Post(title="<post 3>")]
    post_comments: List[List[Comment]] = [[Comment(body="<comment 1a>"), Comment(body="<comment 1b>")], [], [Comment(body="<comment 3a>")]]

    @dmq.root_fetcher(PostQuery, Post)
    class PostFetcher:
        def __call__(self, executor: dmq.Executor, query: PostQuery) -> List[Post]:
            return posts

    @dmq.field_fetcher(CommentQuery, Comment, parent_type=Post)
    class PostCommentFetcher:
        def __call__(self, executor: dmq.Executor, query: CommentQuery, *, parents: List[Post]) -> List[List[Comment]]:
            assert parents == posts
            return post_comments

    @dmq.field_fetcher(UserQuery, User, parent_type=Comment)
    class UserAuthorFetcher:
        def __call__(self, executor: dmq.Executor, query: UserQuery, *, parents: List[Post]) -> List[User]:
            assert parents == [Comment(body="<comment 1a>"), Comment(body="<comment 1b>"), Comment(body="<comment 3a>")]
            return [User(name="<author 1>"), User(name="<author 2>"), User(name="<author 3>")]


    @dataclasses.dataclass(frozen=True)
    class CommentWithAuthor(Comment):
        author: User = dmq.field(query=UserQuery(element_type=User))

    @dataclasses.dataclass(frozen=True)
    class PostWithComments(Post):
        comments: List[Comment] = dmq.field(query=CommentQuery(element_type=CommentWithAuthor))

    executor = dmq.Executor(root_fetchers=[PostFetcher()], field_fetchers=[PostCommentFetcher(), UserAuthorFetcher()])
    posts = executor.fetch(PostQuery(element_type=PostWithComments))

    assert_that(posts, is_sequence(
        has_attrs(
            title="<post 1>",
            comments=is_sequence(
                has_attrs(body="<comment 1a>", author=has_attrs(name="<author 1>")),
                has_attrs(body="<comment 1b>", author=has_attrs(name="<author 2>")),
            ),
        ),
        has_attrs(
            title="<post 2>",
            comments=is_sequence(),
        ),
        has_attrs(
            title="<post 3>",
            comments=is_sequence(
                has_attrs(body="<comment 3a>", author=has_attrs(name="<author 3>")),
            ),
        ),
    ))
