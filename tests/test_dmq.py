import dataclasses
from typing import List, Type, TypeVar

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


def test_can_fetch_extended_objects_from_root() -> None:
    @dataclasses.dataclass(frozen=True)
    class Post:
        title: str

    TPost = TypeVar("TPost", bound=Post)

    @dataclasses.dataclass(frozen=True)
    class PostQuery:
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
        def __call__(self, executor: dmq.Executor, query: PostQuery, *, parents: List[Post]) -> List[Post]:
            return [parent.title.upper() for parent in parents]

    executor = dmq.Executor(root_fetchers=[PostFetcher()], field_fetchers=[PostUppercaseTitleFetcher()])
    posts = executor.fetch(PostQuery(element_type=PostWithUppercaseTitle))

    assert_that(posts, is_sequence(
        has_attrs(title="<post 1>", uppercase_title="<POST 1>"),
        has_attrs(title="<post 2>", uppercase_title="<POST 2>"),
    ))
