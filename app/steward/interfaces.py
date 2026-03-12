from typing import Protocol

from app.steward.models import RegistrySchema


class RegistryReader(Protocol):
    def get_schema_snapshot(self, *, version: str | None = None) -> RegistrySchema:
        ...

class SchemaRegistry(Protocol):
    def get_abstract_schema(self) -> RegistrySchema:
        ...

class DDLWatcher(Protocol):
    def watch(self) -> None:
        ...

class AliasResolver(Protocol):
    """Internal Steward protocol for resolving aliases."""
    def resolve(self, identifier: str) -> str:
        ...
