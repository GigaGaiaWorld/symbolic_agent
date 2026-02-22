# Authoring 层契约（Authoring Contract v1）

目标：在 **不绑定具体 DSL 语法**（Python/YAML/UI 表单均可）的前提下，锁定 Authoring 层概念与当前项目 canonical 契约（SchemaIR / Rule / Derivation / Export/Runner）的映射关系，避免后续 parser/UI 先行导致语义分叉。

本文件是 **Authoring 输入层 → Canonical IR** 的规范；执行层权威语义仍以以下文档为准：

- `/Users/zhenzhili/symbolic_agent/docs/规范.md`（SchemaIR、PredId、IdentityPolicy、group_key_indexes）
- `/Users/zhenzhili/symbolic_agent/docs/断言层 证据层.md`（claim/claim_arg、写入协议、append-only）
- `/Users/zhenzhili/symbolic_agent/docs/视图层.md`（active/chosen、functional/multi/temporal view）
- `/Users/zhenzhili/symbolic_agent/docs/规则.md`（Rule/Derivation、CandidateSet、accept）
- `/Users/zhenzhili/symbolic_agent/docs/导出与运行.md`（Exporter/Runner、outputs_map）

---

## 1. 范围与非目标（v1）

### 1.1 本文件覆盖

- Authoring 概念项（Entity / Identity / Field / Rule 意图）的 canonical 映射
- 命名口径（`pred_id` vs `display_name` / `aliases`）
- `fact_key` 与 `group_key_indexes` 的对应关系
- 逻辑语义与物理实现边界（append-only）
- reification 的触发条件与产物要求
- 面向 Authoring preflight / DTO / session DTO 的诊断码建议

### 1.2 本文件不覆盖

- 具体 Python DSL 语法/元类实现
- UI 页面/交互组件实现
- Rule DSL 全量语法细节（以 `/规则.md` 为准）
- 运行时引擎能力扩展（以 Exporter/Runner 文档为准）

---

## 2. 核心原则（Authoring → Canonical）

1. **Authoring 只是意图层**：输入语法不是执行权威；最终以 SchemaIR / Rule 规范为准。
2. **Identity ≠ fact_key**：身份只用于 EntityRef；`fact_key` 只用于字段事实唯一性分组。
3. **逻辑更新 ≠ 物理覆盖**：`functional` 的“替换”是逻辑语义；物理实现必须 append-only（claim/revokes + policy/view）。
4. **Canonical 名称唯一**：执行层只认 canonical `pred_id`；展示名与 alias 不参与写入、冲突组、导出。
5. **n-ary 是一级能力**：reify 由语义需求驱动，而不是因为底层只能二元。

---

## 3. Authoring 概念到 Canonical IR 的映射（硬约束）

### 3.1 `Entity`

Authoring 中的实体声明（类、表单、YAML 节点等）编译为 SchemaIR 的：

- `entity_type`
- `identity_fields`（有序）
- 可选 metadata（owner/security/docstring 等）
- 若存在 reified relation / materialize_as 配置，则生成对应 projection 信息（见 `/规范.md` 的 reify 与 projection 章节）

硬约束：

- `entity_type` 是 canonical 类型名；后续 `EntityRef` 的 `idref_v1:<entity_type>:...` 必须使用该值。
- Authoring 层不得在运行时按数据内容动态改变 `entity_type`。

### 3.2 `Identity(...)`

Authoring 中标记为身份字段的项，编译为 SchemaIR `identity_fields` 的元素。

映射要求：

- 保留声明顺序（order-sensitive）
- 编译时确定 `type_domain`
- 若有默认值策略（如 `default_factory=uuid4`），仅作为 **authoring/write-time policy metadata**；真正写入后仍需按 canonical identity value 落地并参与 `idref_v1`

硬约束：

- Identity 字段 **不得**同时参与字段事实的 `fact_key` 定义（除非它本身是某个 Field 的 value/dim，且按 Field 语义单独声明）
- Identity 字段不直接生成业务谓词 claim（除非额外被声明为 Field）

### 3.3 `Field(...)`

Authoring 中字段声明编译为 SchemaIR `predicates[]` 的一项（或 reify 展开规则的输入）。

至少映射：

- canonical `pred_id`
- `arg_specs`
- `cardinality`
- `group_key_indexes`
- 可选 metadata（`display_name`, `aliases`, `description`, owner metadata）

硬约束：

- Field 不参与 EntityRef 生成（除非同名字段另行声明为 Identity；v1 不推荐这种重叠设计）
- `cardinality` 只能是 `functional|multi|temporal`
- `type_domain` 必须映射到已锁死 Tag 枚举（`entity_ref|string|int|float64|bool|bytes|time|uuid`）

### 3.4 `Field.fact_key`（Authoring 名） → `SchemaIR.group_key_indexes`（Canonical 名）

这是 v1 的关键收口点。

- Authoring 层可用 **`fact_key`** 表达“functional 的唯一性分组维度”（更贴业务语义）
- Canonical SchemaIR 层统一落成 **`group_key_indexes`**

编译规则（硬约束）：

1. 先确定业务谓词参数序列：`[E] + dims + [value]`（见 `/规范.md`）
2. `fact_key` 默认为 `{E}`（即只含主实体）
3. 若声明额外维度（如 `lang` / `source` / `address_type`），这些维度必须是参数序列中的 dims 子集
4. 编译为 `group_key_indexes` 时：
   - 必须包含 `0`（subject / E）
   - 必须包含所有 `fact_key` 对应 dims 的参数位置
   - 必须 **不包含** value 位置
5. `group_key_indexes` 必须升序、0-based、范围合法

语义等价：

- Authoring `fact_key` 是业务建模表达
- SchemaIR `group_key_indexes` 是执行/冲突组/chosen 的 canonical 表达

### 3.5 `Field.name` / `display_name` / `aliases`

为避免当前项目语义分叉，v1 建议在 Authoring 层拆分三类名字：

- `pred_id`（canonical，执行层唯一权威）
- `display_name`（展示名，仅 UI/文档）
- `aliases[]`（兼容旧名/导入映射）

兼容策略（Authoring 输入可支持语法糖）：

- 若 Authoring 提供 `Field(name="has_age")`，v1 将其解释为 **PredId override 的局部字段名片段**，并编译为 canonical `pred_id="<owner>:has_age"`
- 同时建议编译器内部生成显式 metadata：
  - `display_name`（若有）
  - `aliases`（若有）

硬约束：

- claim/view/export/where 执行统一使用 canonical `pred_id`
- `aliases` 只用于 authoring/import 解析映射，不进入冲突组 key，不进入 `EntityRef`

---

## 4. 基数语义（逻辑）与写入语义（物理）的边界

### 4.1 `functional`

逻辑语义：

- 同一 key 组（由 `group_key_indexes` 决定）下最多一个 current value

物理实现（硬约束）：

- 不能就地覆盖历史记录
- 必须通过 append-only 写入 claim/meta（必要时追加 `revokes/2`）并由 active/chosen 推导 current

### 4.2 `multi`

逻辑语义：

- 同一 key 组下允许多个 value 并存

物理实现：

- append-only 写入；删除/撤销仍走统一 revokes 模型

### 4.3 `temporal`

逻辑语义：

- 历史只追加；`record` 输出保留 active 历史；`current` 由 view/policy 规则推导

物理实现：

- 禁止物理覆盖历史
- `current` 不是写入协议行为，而是视图/导出/runner 输出行为（当前项目已通过 `outputs_map` 承载双输出）

---

## 5. Reification（关系实体化）契约（v1）

### 5.1 何时必须 reify（Authoring 语义判断）

以下情形应优先 reify：

- 关系本身需要身份/生命周期（可独立被引用、审计、撤销）
- 关系需要挂属性（例如 `since/title/source/confidence`）
- 需要把同一关系实例作为多个规则/决策的主语对象

### 5.2 reify 的 canonical 产物（SchemaIR / 写入侧必须可用）

Authoring 层若声明 reified relation / relation-entity，SchemaCompiler 必须在 SchemaIR 中生成：

- record entity type（例如 `Employment`）
- `<T>:exists`（record existence predicate）
- 角色谓词（role predicates，如 `employment:employee`, `employment:employer`）
- 属性谓词（如 `employment:since`, `employment:title`）
- （若允许 fact materialize）projection 信息：`projection_pred_id`, `projection_arg_order`

硬约束：

- accept 写回与 FactCompiler 必须复用同一组 canonical 谓词名；禁止重复发明

---

## 6. Rule / Derivation 的 Authoring 契约边界（v1）

Authoring 层可表达规则/推导意图，但 canonical 执行契约仍由 `RuleSpec` / where DSL / CandidateSet 决定。

建议收口：

- Authoring Rule 的 where/constraint 语义最终编译为当前 where DSL 子集（`pred/eq/in/cmp/not` 等）
- `temporal_view` 必须显式选择（默认 `record`）
- Authoring 层若选择 `temporal_view="current"`，但 where 未引用 temporal 谓词，应允许 preflight 给出 warning（当前已实现）

---

## 7. Authoring Diagnostics / Warnings 契约（对齐现有 preflight/DTO）

### 7.1 现有已落地（可复用）

`authoring_preflight_v1` / `authoring_ui_dto_v1` 当前已支持：

- 结构化 `diagnostics[]`: `phase/code/path/message/severity`
- `warnings[]`
- session 级聚合（`authoring_session_dto_v1`）
- 代码侧 canonical code registry（`factpy_kernel/authoring/diagnostic_codes.py`）作为实现与测试的唯一基准

现有 warning/code 示例（已实装）：

- `empty_predicates`
- `souffle_binary_missing`
- `temporal_current_no_pred_refs`
- `temporal_current_no_temporal_schema_predicates`
- `temporal_current_no_temporal_where_predicates`
- `preview_truncated`

### 7.2 v1 建议预留（尚未实装）

为后续 DSL/Editor 实装建议预留 code（不要求本轮实现）：

- `authoring.fact_key_invalid`
- `authoring.fact_key_refs_unknown_dim`
- `authoring.pred_id_alias_conflict`
- `authoring.identity_field_overlaps_fact_key`
- `authoring.reify_missing_role_field`
- `authoring.display_name_conflicts_alias`

建议路径口径：

- schema 概念层：`$.entities[i]...` / `$.predicates[i]...`
- authoring 输入层（DSL/表单）：`$.entity_defs[i]...` / `$.rule_defs[i]...`
- 若来自 parser，可加 `source_span`（后续版本）

---

## 8. 版本化与兼容策略（Authoring Contract v1）

- 本文件定义的是 **语义映射契约 v1**，不是具体 DSL 语法版本
- 后续新增 DSL 语法糖（例如 decorator / class syntax）只要编译结果满足本契约，不构成不兼容变更
- 以下变更视为不兼容（需 bump contract version）：
  - `fact_key -> group_key_indexes` 映射规则变化
  - `pred_id` canonical 命名口径变化
  - `functional/multi/temporal` 逻辑语义变化
  - reify canonical 产物命名规则变化

---

## 9. Golden Fixtures（引用）

本契约的示例与回归基线见：

- `/Users/zhenzhili/symbolic_agent/docs/Authoring 层契约 fixtures.md`

这些 fixtures 仅用于固定语义映射与诊断/DTO 契约；**不是** DSL 实现样例代码的权威格式。
