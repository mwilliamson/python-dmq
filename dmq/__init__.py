import dataclasses
from typing import Any, Dict, Generic, List, Tuple, Type, TypeVar


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

    def _fetch_core(self, query: Query[T]) -> Tuple[Type[T], List[T]]:
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
