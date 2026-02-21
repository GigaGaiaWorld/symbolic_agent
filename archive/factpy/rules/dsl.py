"""Rule DSL primitives for Horn-clause authoring."""

from __future__ import annotations

from dataclasses import dataclass
import dis
import inspect
import re
from string import ascii_lowercase
from typing import Any, Iterator


@dataclass(frozen=True, eq=False)
class LogicVar:
    name: str

    def _cmp(self, op: str, other: object) -> BuiltinExpr:
        return BuiltinExpr(op=op, left=self, right=other)

    def __eq__(self, other: object) -> BuiltinExpr:  # type: ignore[override]
        return self._cmp("eq", other)

    def __ne__(self, other: object) -> BuiltinExpr:  # type: ignore[override]
        return self._cmp("neq", other)

    def __lt__(self, other: object) -> BuiltinExpr:
        return self._cmp("lt", other)

    def __le__(self, other: object) -> BuiltinExpr:
        return self._cmp("le", other)

    def __gt__(self, other: object) -> BuiltinExpr:
        return self._cmp("gt", other)

    def __ge__(self, other: object) -> BuiltinExpr:
        return self._cmp("ge", other)


@dataclass(frozen=True)
class PredicateCall:
    predicate: str
    args: tuple[object, ...]
    source_entity: str | None = None


@dataclass(frozen=True)
class PredicateFactory:
    entity_cls: type
    predicate_name: str

    def __call__(self, *args: object) -> PredicateCall:
        base_name = f"{self.entity_cls.__name__.lower()}_{self.predicate_name}"
        return PredicateCall(predicate=base_name, args=tuple(args), source_entity=self.entity_cls.__name__)


@dataclass(frozen=True)
class PathExpression:
    entity_cls: type
    subject: object
    field_name: str

    def __eq__(self, other: object) -> FieldConstraint:  # type: ignore[override]
        return FieldConstraint(path=self, op="eq", target=other)

    def __ne__(self, other: object) -> FieldConstraint:  # type: ignore[override]
        return FieldConstraint(path=self, op="neq", target=other)

    def in_(self, values: list[object] | tuple[object, ...] | set[object]) -> InConstraint:
        return InConstraint(path=self, values=tuple(values))


@dataclass(frozen=True)
class FieldConstraint:
    path: PathExpression
    op: str
    target: object


@dataclass(frozen=True)
class InConstraint:
    path: PathExpression
    values: tuple[object, ...]


@dataclass(frozen=True)
class BuiltinExpr:
    op: str
    left: object
    right: object


@dataclass(frozen=True)
class EntityBinding:
    entity_cls: type
    subject: object

    def __getattr__(self, field_name: str) -> PathExpression:
        specs = getattr(self.entity_cls, "__field_specs__", {})
        if field_name not in specs:
            raise AttributeError(f"{self.entity_cls.__name__} has no field '{field_name}'")
        return PathExpression(entity_cls=self.entity_cls, subject=self.subject, field_name=field_name)


@dataclass(frozen=True)
class StructuredEntityExpression:
    entity_cls: type
    field_terms: dict[str, object]


@dataclass(frozen=True)
class Rule:
    head: object
    body: tuple[object, ...]

    def __init__(self, *, head: object, body: list[object] | tuple[object, ...]) -> None:
        object.__setattr__(self, "head", head)
        object.__setattr__(self, "body", tuple(body))


class _VarsContext:
    def __init__(self, names: tuple[str, ...] | None = None) -> None:
        self._names = names

    def __enter__(self) -> tuple[LogicVar, ...]:
        names = self._names
        if names is None:
            inferred = self._infer_unpack_size()
            names = tuple(_default_var_name(idx) for idx in range(inferred))
        return tuple(LogicVar(name=item) for item in names)

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def _infer_unpack_size(self) -> int:
        frame = inspect.currentframe()
        if frame is None or frame.f_back is None or frame.f_back.f_back is None:
            return 4
        caller = frame.f_back.f_back

        frame_info = inspect.getframeinfo(caller)
        if frame_info.code_context:
            line = frame_info.code_context[0]
            match = re.search(r"\\bas\\s*\\(([^)]*)\\)", line)
            if match:
                raw_items = match.group(1).split(",")
                count = sum(1 for item in raw_items if item.strip())
                if count > 0:
                    return count

        instructions = list(dis.get_instructions(caller.f_code))
        for inst in instructions:
            if inst.offset <= caller.f_lasti:
                continue
            if inst.opname == "UNPACK_SEQUENCE" and isinstance(inst.arg, int):
                return max(1, inst.arg)
            if inst.opname == "UNPACK_EX" and isinstance(inst.arg, int):
                before = inst.arg & 0xFF
                after = inst.arg >> 8
                return max(1, before + after + 1)
        return 4


def _default_var_name(idx: int) -> str:
    if idx < len(ascii_lowercase):
        return ascii_lowercase[idx]
    return f"v{idx + 1}"


def vars(*names: str) -> _VarsContext:
    if names:
        normalized: list[str] = []
        for item in names:
            if not isinstance(item, str) or not item.strip():
                raise ValueError("vars(...) only accepts non-empty string names.")
            normalized.append(item.strip())
        return _VarsContext(tuple(normalized))
    return _VarsContext(None)


def is_logic_object(value: object) -> bool:
    return isinstance(
        value,
        (
            LogicVar,
            PredicateCall,
            PathExpression,
            FieldConstraint,
            InConstraint,
            BuiltinExpr,
            EntityBinding,
            StructuredEntityExpression,
        ),
    )


def should_route_entity_call(args: tuple[object, ...], kwargs: dict[str, object]) -> bool:
    if len(args) == 1 and not kwargs and (
        is_logic_object(args[0]) or isinstance(args[0], (str, int, float, bool))
    ):
        return True
    if not args and kwargs and any(is_logic_object(value) for value in kwargs.values()):
        return True
    return False


def build_entity_expression(entity_cls: type, *, args: tuple[object, ...], kwargs: dict[str, object]) -> object:
    if len(args) == 1 and not kwargs:
        return EntityBinding(entity_cls=entity_cls, subject=args[0])
    if kwargs:
        return StructuredEntityExpression(entity_cls=entity_cls, field_terms=dict(kwargs))
    raise ValueError("Unsupported DSL entity expression.")


def iter_logic_vars(values: tuple[object, ...] | list[object]) -> Iterator[LogicVar]:
    for item in values:
        if isinstance(item, LogicVar):
            yield item
