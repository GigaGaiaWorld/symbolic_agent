"""FactPy schema authoring layer (Entity/Identity/Field)."""

from __future__ import annotations

from dataclasses import dataclass, field
import inspect
import unicodedata
import uuid
from typing import TYPE_CHECKING, Any, Callable, ClassVar, Literal

if TYPE_CHECKING:  # pragma: no cover
    from .compiler import Store


Cardinality = Literal["functional", "multi", "temporal"]
TemporalMode = Literal["valid_time", "versioned"]
FieldOperationKind = Literal["set", "add", "remove"]


class FactPySchemaError(ValueError):
    """Raised when schema declarations are invalid."""


def _normalize_name(name: object, *, label: str) -> str:
    if not isinstance(name, str) or not name.strip():
        raise FactPySchemaError(f"{label} must be a non-empty string.")
    return name.strip()


def _canonicalize_identity_value(value: object) -> object:
    if isinstance(value, str):
        return unicodedata.normalize("NFC", value).strip()
    return value


def _resolve_default_factory(factory: str | Callable[[], object] | None) -> Callable[[], object] | None:
    if factory is None:
        return None
    if isinstance(factory, str):
        normalized = factory.strip().lower()
        if normalized == "uuid4":
            return lambda: str(uuid.uuid4())
        raise FactPySchemaError(f"Unsupported default_factory string: {factory}")
    if callable(factory):
        return factory
    raise FactPySchemaError("default_factory must be callable, 'uuid4', or None.")


class Identity:
    """Identity descriptor for EntityRef construction."""

    def __init__(
        self,
        *,
        canonicalizer: Callable[[object], object] | None = None,
        default_factory: str | Callable[[], object] | None = None,
        description: str | None = None,
    ) -> None:
        self.canonicalizer = canonicalizer or _canonicalize_identity_value
        self.default_factory = _resolve_default_factory(default_factory)
        self.description = description
        self.name: str | None = None
        self.annotation: Any = Any

    def __set_name__(self, owner: type, name: str) -> None:
        self.name = name

    def __get__(self, instance: Entity | None, owner: type | None = None) -> object:
        if instance is None:
            return self
        if self.name is None:
            raise AttributeError("Identity not initialized.")
        return instance._identity_values.get(self.name)

    def __set__(self, instance: Entity, value: object) -> None:
        if self.name is None:
            raise AttributeError("Identity not initialized.")
        if instance._sealed:
            raise FactPySchemaError(f"Identity '{self.name}' is immutable after entity creation.")
        instance._identity_values[self.name] = self.canonicalizer(value)


@dataclass(frozen=True)
class PendingFieldOperation:
    field_name: str
    operation: FieldOperationKind
    payload: object
    meta: dict[str, object] = field(default_factory=dict)


class BoundField:
    """Per-instance field accessor with set/add/remove operations."""

    def __init__(self, instance: Entity, spec: Field) -> None:
        self._instance = instance
        self._spec = spec

    @property
    def value(self) -> object:
        if self._spec.name is None:
            raise AttributeError("Field not initialized.")
        return self._instance._field_values.get(self._spec.name)

    def set(self, value: object, *, meta: dict[str, object] | None = None) -> None:
        if self._spec.name is None:
            raise AttributeError("Field not initialized.")
        self._instance._field_set(self._spec.name, value, meta=meta)

    def add(self, value: object, *, meta: dict[str, object] | None = None) -> None:
        if self._spec.name is None:
            raise AttributeError("Field not initialized.")
        if self._spec.cardinality == "functional":
            raise FactPySchemaError(
                f"Field '{self._spec.name}' is functional. Use set() instead of add()."
            )
        self._instance._field_add(self._spec.name, value, meta=meta)

    def remove(self, value: object | None = None, *, meta: dict[str, object] | None = None) -> None:
        if self._spec.name is None:
            raise AttributeError("Field not initialized.")
        self._instance._field_remove(self._spec.name, value, meta=meta)

    def __repr__(self) -> str:
        return f"BoundField(name={self._spec.name!r}, value={self.value!r})"


class Field:
    """Fact field descriptor."""

    def __init__(
        self,
        *,
        name: str | None = None,
        cardinality: Cardinality = "functional",
        fact_key: list[str] | tuple[str, ...] | None = None,
        temporal_mode: TemporalMode | None = None,
        aliases: list[str] | tuple[str, ...] | None = None,
        description: str | None = None,
    ) -> None:
        if cardinality not in {"functional", "multi", "temporal"}:
            raise FactPySchemaError("Field cardinality must be functional|multi|temporal.")
        if temporal_mode is not None and temporal_mode not in {"valid_time", "versioned"}:
            raise FactPySchemaError("temporal_mode must be valid_time|versioned when provided.")
        if cardinality == "temporal" and temporal_mode is None:
            raise FactPySchemaError("temporal fields must set temporal_mode.")
        if cardinality != "temporal" and temporal_mode is not None:
            raise FactPySchemaError("temporal_mode is only valid when cardinality='temporal'.")

        normalized_fact_key: tuple[str, ...] = tuple()
        if fact_key is not None:
            if not isinstance(fact_key, (list, tuple)):
                raise FactPySchemaError("fact_key must be list/tuple of dim names.")
            normalized_fact_key = tuple(_normalize_name(item, label="fact_key item") for item in fact_key)
            if len(set(normalized_fact_key)) != len(normalized_fact_key):
                raise FactPySchemaError("fact_key contains duplicated dimensions.")

        normalized_aliases: tuple[str, ...] = tuple()
        if aliases is not None:
            if not isinstance(aliases, (list, tuple)):
                raise FactPySchemaError("aliases must be list/tuple of strings.")
            normalized_aliases = tuple(_normalize_name(item, label="alias") for item in aliases)
            if len(set(normalized_aliases)) != len(normalized_aliases):
                raise FactPySchemaError("aliases contain duplicates.")

        self.predicate_name = _normalize_name(name, label="Field name") if name is not None else None
        self.cardinality: Cardinality = cardinality
        self.fact_key: tuple[str, ...] = normalized_fact_key
        self.temporal_mode: TemporalMode | None = temporal_mode
        self.aliases: tuple[str, ...] = normalized_aliases
        self.description = description
        self.name: str | None = None
        self.annotation: Any = Any

    def __set_name__(self, owner: type, name: str) -> None:
        self.name = name

    def __get__(self, instance: Entity | None, owner: type | None = None) -> object:
        if instance is None:
            return self
        return BoundField(instance, self)

    def __set__(self, instance: Entity, value: object) -> None:
        if self.name is None:
            raise AttributeError("Field not initialized.")
        instance._field_set(self.name, value, meta=None)


@dataclass(frozen=True)
class ValidTimeValue:
    """Payload for temporal valid_time fields."""

    value: object
    start: object
    end: object | None = None
    dims: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class VersionedValue:
    """Payload for temporal versioned fields."""

    value: object
    version: object
    dims: dict[str, object] = field(default_factory=dict)


class EntityMeta(type):
    """Collect Identity/Field descriptors and Meta/docstring metadata."""

    def __call__(cls, *args, **kwargs):  # type: ignore[override]
        try:
            from .rules.dsl import build_entity_expression, should_route_entity_call
        except Exception:
            return super().__call__(*args, **kwargs)

        if should_route_entity_call(args, kwargs):
            return build_entity_expression(cls, args=args, kwargs=kwargs)
        return super().__call__(*args, **kwargs)

    def __getattr__(cls, name: str) -> object:
        if name.startswith("_"):
            raise AttributeError(name)
        try:
            from .rules.dsl import PredicateFactory
        except Exception as exc:  # pragma: no cover - fallback for import cycle edges
            raise AttributeError(name) from exc
        return PredicateFactory(entity_cls=cls, predicate_name=name)

    def __new__(mcls, name, bases, namespace):
        cls = super().__new__(mcls, name, bases, namespace)
        if name == "Entity" and cls.__module__ == __name__:
            cls.__identity_specs__ = {}
            cls.__field_specs__ = {}
            cls.__meta__ = {}
            cls.__doc_meta__ = ""
            return cls

        annotations: dict[str, object] = {}
        for base in reversed(bases):
            annotations.update(getattr(base, "__annotations__", {}))
        annotations.update(namespace.get("__annotations__", {}))

        identity_specs: dict[str, Identity] = {}
        field_specs: dict[str, Field] = {}
        for attr_name, annotation in annotations.items():
            descriptor = getattr(cls, attr_name, None)
            if isinstance(descriptor, Identity):
                descriptor.annotation = annotation
                identity_specs[attr_name] = descriptor
                continue
            if isinstance(descriptor, Field):
                descriptor.annotation = annotation
                field_specs[attr_name] = descriptor
                continue

        if not identity_specs:
            raise FactPySchemaError(f"Entity '{name}' requires at least one Identity field.")

        meta_class = namespace.get("Meta")
        meta_payload: dict[str, object] = {}
        if meta_class is not None:
            for key in dir(meta_class):
                if key.startswith("_"):
                    continue
                meta_payload[key] = getattr(meta_class, key)

        cls.__identity_specs__ = identity_specs
        cls.__field_specs__ = field_specs
        cls.__meta__ = meta_payload
        cls.__doc_meta__ = inspect.cleandoc(namespace.get("__doc__", "") or "")
        return cls


class Entity(metaclass=EntityMeta):
    """Entity base class."""

    __identity_specs__: ClassVar[dict[str, Identity]]
    __field_specs__: ClassVar[dict[str, Field]]
    __meta__: ClassVar[dict[str, object]]
    __doc_meta__: ClassVar[str]

    def __init__(self, **kwargs: object) -> None:
        unknown = sorted(
            set(kwargs.keys()) - set(self.__identity_specs__.keys()) - set(self.__field_specs__.keys())
        )
        if unknown:
            raise FactPySchemaError(f"Unknown constructor fields for {self.__class__.__name__}: {unknown}")

        self._identity_values: dict[str, object] = {}
        self._field_values: dict[str, object] = {}
        self._sealed = False
        self._pending_ops: list[PendingFieldOperation] = []

        for key, spec in self.__identity_specs__.items():
            if key in kwargs:
                value = kwargs[key]
            elif spec.default_factory is not None:
                value = spec.default_factory()
            else:
                raise FactPySchemaError(
                    f"Missing Identity value for {self.__class__.__name__}.{key}"
                )
            self._identity_values[key] = spec.canonicalizer(value)

        for key, spec in self.__field_specs__.items():
            if key not in kwargs:
                continue
            provided = kwargs[key]
            if spec.cardinality == "multi":
                values = provided if isinstance(provided, list) else [provided]
                self._field_values[key] = list(values)
                for item in values:
                    self._record_operation(key, "add", item, meta=None)
                continue
            if spec.cardinality == "temporal":
                self._field_values[key] = provided
                if isinstance(provided, list):
                    for item in provided:
                        self._record_operation(key, "add", item, meta=None)
                else:
                    self._record_operation(key, "set", provided, meta=None)
                continue
            self._field_values[key] = provided
            self._record_operation(key, "set", provided, meta=None)

        self._sealed = True

    @property
    def identity_values(self) -> dict[str, object]:
        return dict(self._identity_values)

    @property
    def field_values(self) -> dict[str, object]:
        return dict(self._field_values)

    def _record_operation(
        self,
        field_name: str,
        operation: FieldOperationKind,
        payload: object,
        *,
        meta: dict[str, object] | None,
    ) -> None:
        self._pending_ops.append(
            PendingFieldOperation(
                field_name=field_name,
                operation=operation,
                payload=payload,
                meta=dict(meta or {}),
            )
        )

    def _field_set(self, field_name: str, value: object, *, meta: dict[str, object] | None) -> None:
        spec = self.__field_specs__[field_name]
        if spec.cardinality == "multi":
            self._field_values[field_name] = list(value) if isinstance(value, list) else [value]
        else:
            self._field_values[field_name] = value
        self._record_operation(field_name, "set", value, meta=meta)

    def _field_add(self, field_name: str, value: object, *, meta: dict[str, object] | None) -> None:
        spec = self.__field_specs__[field_name]
        if spec.cardinality == "functional":
            raise FactPySchemaError(f"Field '{field_name}' is functional and cannot add().")

        if spec.cardinality == "multi":
            current = self._field_values.get(field_name)
            if current is None:
                current = []
                self._field_values[field_name] = current
            if not isinstance(current, list):
                current = [current]
                self._field_values[field_name] = current
            current.append(value)
        else:
            current = self._field_values.get(field_name)
            if current is None:
                self._field_values[field_name] = [value]
            elif isinstance(current, list):
                current.append(value)
            else:
                self._field_values[field_name] = [current, value]

        self._record_operation(field_name, "add", value, meta=meta)

    def _field_remove(
        self,
        field_name: str,
        value: object | None,
        *,
        meta: dict[str, object] | None,
    ) -> None:
        spec = self.__field_specs__[field_name]
        current = self._field_values.get(field_name)

        if value is None:
            if spec.cardinality == "multi":
                targets = list(current) if isinstance(current, list) else ([] if current is None else [current])
                for item in targets:
                    self._record_operation(field_name, "remove", item, meta=meta)
                self._field_values[field_name] = []
                return
            if current is None:
                raise FactPySchemaError(f"Field '{field_name}' has no value to remove.")
            value = current

        if spec.cardinality == "multi":
            if isinstance(current, list):
                try:
                    current.remove(value)
                except ValueError:
                    pass
            elif current == value:
                self._field_values[field_name] = []
        elif current == value:
            self._field_values[field_name] = None

        self._record_operation(field_name, "remove", value, meta=meta)

    def _pending_operations(self) -> list[tuple[str, FieldOperationKind, object, dict[str, object]]]:
        return [
            (item.field_name, item.operation, item.payload, dict(item.meta))
            for item in self._pending_ops
        ]

    def _clear_pending_operations(self) -> None:
        self._pending_ops.clear()

    def save(
        self,
        *,
        meta: dict[str, object] | None = None,
        store: Store | None = None,
    ) -> list[str]:
        if store is None:
            from .compiler import get_active_store

            store = get_active_store()
        if store is None:
            raise FactPySchemaError("No active FactPy store. Pass store=... or use factpy.batch(...).")
        return store.save(self, meta=meta)
