from typing import Protocol

from app.steward import RegistrySchema


class RegistryReader(Protocol):
    def get_schema_snapshot(self, *, version: str | None = None) -> RegistrySchema:
        ...

class AliasResolver(Protocol):
    """Internal Steward protocol for resolving aliases."""
    def resolve(self, identifier: str) -> str:
        ...
