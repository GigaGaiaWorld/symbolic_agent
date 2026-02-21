"""ER helper compiler for external source-key bridges."""

from __future__ import annotations

from dataclasses import dataclass

from ..compiler import CanonicalTupleCodec, EntityRef, FactPyCompileError, Store
from ..model import Entity


@dataclass(frozen=True)
class BridgePredicates:
    key_to_mention: str = "er:key_to_mention"
    key_to_canon: str = "er:key_to_canon"


class ERCompiler:
    """Utilities to emit source-key bridge predicates into claim store."""

    def __init__(self, *, predicates: BridgePredicates | None = None) -> None:
        self.predicates = predicates or BridgePredicates()

    @staticmethod
    def encode_source_key(*parts: object) -> str:
        if not parts:
            raise FactPyCompileError("encode_source_key requires at least one key part.")
        return CanonicalTupleCodec.encode(parts)

    def emit_key_to_mention(
        self,
        *,
        store: Store,
        source_key: str | tuple[object, ...] | list[object],
        mention: Entity | EntityRef | str,
        meta: dict[str, object] | None = None,
    ) -> str:
        key = self._normalize_source_key(source_key)
        return store.emit(
            pred=self.predicates.key_to_mention,
            s=key,
            o=[mention],
            meta=meta,
        )

    def emit_key_to_canon(
        self,
        *,
        store: Store,
        source_key: str | tuple[object, ...] | list[object],
        canonical: Entity | EntityRef | str,
        meta: dict[str, object] | None = None,
    ) -> str:
        key = self._normalize_source_key(source_key)
        return store.emit(
            pred=self.predicates.key_to_canon,
            s=key,
            o=[canonical],
            meta=meta,
        )

    def _normalize_source_key(self, source_key: str | tuple[object, ...] | list[object]) -> str:
        if isinstance(source_key, str):
            if source_key.startswith("tup1:"):
                return source_key
            return CanonicalTupleCodec.encode([source_key])
        if isinstance(source_key, (tuple, list)):
            return CanonicalTupleCodec.encode(list(source_key))
        raise FactPyCompileError(
            f"source_key must be encoded string or tuple/list, got {type(source_key).__name__}."
        )
