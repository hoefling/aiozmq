from typing import Any, Collection, FrozenSet, Iterator, TypeVar

_T = TypeVar('_T')


class _EndpointsSet(FrozenSet[_T]):

    __slots__ = ('_collection',)

    def __init__(self, collection: Collection[_T]):
        self._collection = collection

    def __len__(self) -> int:
        return len(self._collection)

    def __contains__(self, endpoint: Any) -> bool:
        return endpoint in self._collection

    def __iter__(self) -> Iterator[_T]:
        return iter(self._collection)

    def __repr__(self) -> str:
        return '{' + ', '.join(repr(c) for c in sorted(self._collection)) + '}'

    __str__ = __repr__
