# Authoring 层契约 fixtures（Golden Fixtures v1）

目的：为 `/Users/zhenzhili/symbolic_agent/docs/Authoring 层契约.md` 提供 **语义映射回归样例**。本文件只锁定：

- Authoring 概念输入（伪代码/伪 DSL）
- 期望 Canonical SchemaIR 片段（关键字段）
- 期望 Authoring preflight / DTO / session DTO 的关键输出断言

不要求当前项目已实现 Python DSL parser；这些样例是未来 DSL/YAML/UI Editor 的共同编译基线。

---

## F1. 基础实体 + functional/multi 字段（Person / Company）

### F1-A Authoring 概念输入（伪表示）

```python
class Person(Entity):
    source_system: str = Identity()
    source_id: str = Identity()
    name: str = Field(cardinality="multi")
    age: int = Field(name="has_age", cardinality="functional")
    works_at: Company = Field(cardinality="multi")

class Company(Entity):
    source_system: str = Identity()
    source_id: str = Identity()
    sector: str = Field(cardinality="functional")
```

### F1-B 期望 SchemaIR 关键片段（canonical）

```json
{
  "entities": [
    {"entity_type": "Person", "identity_fields": [{"name": "source_system"}, {"name": "source_id"}]},
    {"entity_type": "Company", "identity_fields": [{"name": "source_system"}, {"name": "source_id"}]}
  ],
  "predicates": [
    {"pred_id": "person:name", "cardinality": "multi", "group_key_indexes": [0]},
    {"pred_id": "person:has_age", "cardinality": "functional", "group_key_indexes": [0]},
    {"pred_id": "person:works_at", "cardinality": "multi", "group_key_indexes": [0]},
    {"pred_id": "company:sector", "cardinality": "functional", "group_key_indexes": [0]}
  ]
}
```

### F1-C 关键断言（preflight / DTO）

- `schema_preflight.ok == true`
- `schema_preflight.summary.predicate_count == 4`
- `schema_preflight.summary.pred_ids` 包含 `person:has_age`
- 若使用 `build_schema_preflight_dto(...)`：
  - `status == "ok"`
  - `source_kind == "schema"`

---

## F2. 带维度的 functional 字段（fact_key → group_key_indexes）

### F2-A Authoring 概念输入（伪表示）

```python
class Person(Entity):
    source_system: str = Identity()
    source_id: str = Identity()

    # 每种语言一个 current name
    name_by_lang: str = Field(
        cardinality="functional",
        fact_key=["lang"],
        dims=[("lang", "string")]
    )
```

> 注：`fact_key=["lang"]` 是 Authoring 层语义名；canonical SchemaIR 必须落为 `group_key_indexes`。

### F2-B 期望 SchemaIR 关键片段（canonical）

```json
{
  "predicates": [
    {
      "pred_id": "person:name_by_lang",
      "arg_specs": [
        {"name": "person", "type_domain": "entity_ref"},
        {"name": "lang", "type_domain": "string"},
        {"name": "value", "type_domain": "string"}
      ],
      "cardinality": "functional",
      "group_key_indexes": [0, 1]
    }
  ]
}
```

### F2-C 关键断言（语义）

- `group_key_indexes` **必须**包含 `0`（E）
- `group_key_indexes` **必须**包含 `lang` 位置（`1`）
- `group_key_indexes` **不得**包含 value 位置（`2`）
- chosen / conflict 分组必须按 `(PredId, E, Lang)`，而非 `(PredId, E)` 或 `(PredId, E, Lang, Name)`

---

## F3. Reified relation（Employment）

### F3-A Authoring 概念输入（伪表示）

```python
class Employment(Entity):
    uid: str = Identity(default_factory="uuid4")
    employee: Person = Field(cardinality="functional")
    employer: Company = Field(cardinality="functional")
    since: int = Field(cardinality="functional")
    title: str = Field(cardinality="functional")
```

### F3-B 期望 SchemaIR 关键片段（canonical）

```json
{
  "entities": [
    {"entity_type": "Employment", "identity_fields": [{"name": "uid", "type_domain": "uuid"}]}
  ],
  "predicates": [
    {"pred_id": "employment:exists", "cardinality": "functional", "group_key_indexes": [0]},
    {"pred_id": "employment:employee", "cardinality": "functional", "group_key_indexes": [0]},
    {"pred_id": "employment:employer", "cardinality": "functional", "group_key_indexes": [0]},
    {"pred_id": "employment:since", "cardinality": "functional", "group_key_indexes": [0]},
    {"pred_id": "employment:title", "cardinality": "functional", "group_key_indexes": [0]}
  ]
}
```

### F3-C 关键断言（语义）

- reified relation 必须生成 `<T>:exists`
- role predicates 与属性 predicates 必须使用同一 canonical 前缀/命名策略
- `uid` 默认值策略属于 authoring/write-time policy；一旦生成并落库，EntityRef 必须稳定

---

## F4. `aliases` / `display_name` 与 canonical `pred_id`

### F4-A Authoring 概念输入（伪表示）

```python
phone: str = Field(
    pred_id="person:phone",
    display_name="Phone",
    aliases=["mobile", "handy"],
    cardinality="multi"
)
```

### F4-B 期望 SchemaIR 关键片段（canonical）

```json
{
  "predicates": [
    {
      "pred_id": "person:phone",
      "cardinality": "multi",
      "group_key_indexes": [0],
      "aliases": ["mobile", "handy"],
      "display_name": "Phone"
    }
  ]
}
```

### F4-C 关键断言（执行边界）

- 写入 claim / view / export / where 编译统一使用 `person:phone`
- alias 仅用于导入解析映射或 authoring 搜索/兼容
- alias 不进入 `group_key_indexes` / conflict key / EntityRef

---

## F5. Authoring preflight / DTO / session DTO 聚合（当前已实装契约）

### F5-A schema preflight（warning）

输入：空 `predicates` 的 SchemaIR（但结构合法）

期望关键输出：

- `authoring_preflight_v1.kind == "schema"`
- `ok == true`
- `warnings[0].code == "empty_predicates"`
- `authoring_ui_dto_v1.status == "warning"`（经 `build_schema_preflight_dto` 包装）

### F5-B derivation preview（warning：preview truncation）

输入：某个谓词可返回 >20 行 candidate 的 derivation preview

期望关键输出：

- `authoring_preflight_v1.kind == "derivation_dry_run"`
- `summary.preview_limit == 20`
- `warnings` 包含 `preview_truncated`
- `authoring_ui_dto_v1.status == "warning"`

### F5-C session DTO 聚合（ok / warning / error）

通过 `build_authoring_session_dto(...)` 聚合 `schema/rule/derivation`：

- 任一 section `status=="error"` → session `status=="error"`
- 无 error 且任一 warning → session `status=="warning"`
- 全部 ok 且无 warning → session `status=="ok"`

期望关键输出：

```json
{
  "authoring_session_dto_version": "authoring_session_dto_v1",
  "kind": "authoring_session",
  "status": "warning",
  "summary": {
    "section_count": 2,
    "diagnostic_count": 0,
    "warning_count": 2,
    "status_counts": {"ok": 0, "warning": 2, "error": 0}
  }
}
```

---

## F6. 反例清单（用于后续 DSL 编译器 diagnostics）

以下反例建议在未来 DSL 编译器 / authoring parser 中作为固定回归项：

1. `fact_key` 引用未知维度（例如 `fact_key=["lang"]` 但未声明 `lang`）
2. `fact_key` 把 value 列误纳入 key（应拒绝）
3. `aliases` 与 canonical `pred_id` 冲突（重复/歧义）
4. Identity 字段与 Field 角色重叠且未显式声明策略（建议 warning 或 error）
5. `temporal_view="current"` 但 schema/where 不涉及 temporal 谓词（当前 preflight 已给 warning）

这些反例暂不要求本轮实现 parser，只要求后续 diagnostics code 与 `/Users/zhenzhili/symbolic_agent/docs/Authoring 层契约.md` 第 7 节口径一致。
