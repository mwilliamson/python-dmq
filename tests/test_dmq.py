import dataclasses
from typing import List

from precisely import assert_that, equal_to

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
