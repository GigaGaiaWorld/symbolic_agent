# KG-Gen Code Walkthrough (Code-Level)

This document explains how `kg-gen` works internally at code level, based on the local source in this repo.

## 1) Core Entry and Data Model

Main entry:
- `src/kg_gen/kg_gen.py:28` `class KGGen`
- `src/kg_gen/kg_gen.py:155` `KGGen.generate(...)`

Core graph object:
- `src/kg_gen/models.py:7` `class Graph`

`Graph` has:
- `entities: set[str]`
- `edges: set[str]`
- `relations: set[tuple[str, str, str]]` as `(subject, predicate, object)`
- optional `entity_clusters`, `edge_clusters`, `entity_metadata`

Important invariant repair:
- `Graph.from_file()` ensures entities/edges include values referenced by relations (`src/kg_gen/models.py:20`).

## 2) Model Initialization and Runtime Context

Initialization flow:
- `KGGen.__init__()` calls `init_model()` (`src/kg_gen/kg_gen.py:29`, `src/kg_gen/kg_gen.py:76`).
- `init_model()` builds a `dspy.LM` instance and stores it in `self.lm`.

Behavior details:
- GPT-5 guardrails:
  - `temperature >= 1.0` (`src/kg_gen/kg_gen.py:68`)
  - `max_tokens >= 16000` (`src/kg_gen/kg_gen.py:72`)
- If `retrieval_model` string is provided, it is loaded as `SentenceTransformer` (`src/kg_gen/kg_gen.py:112`).
- For OpenAI models, `model_type="responses"`; otherwise `"chat"` (`src/kg_gen/kg_gen.py:130`, `src/kg_gen/kg_gen.py:142`).

## 3) End-to-End Generation Pipeline

Top-level pipeline inside `generate()`:
1. Normalize input (plain text or messages array)
2. Extract entities
3. Extract relations
4. Build `Graph` (derive `edges` from relation predicates)
5. Optional deduplication
6. Optional export to JSON

Reference:
- `src/kg_gen/kg_gen.py:155`
- graph build at `src/kg_gen/kg_gen.py:269`
- dedup call at `src/kg_gen/kg_gen.py:275`

### 3.1 Input Normalization

If `input_data` is a list of messages:
- requires each message has `role` and `content`
- keeps only `role in ["user", "assistant"]`
- converts to `"role: content"` lines joined with newline

Reference:
- `src/kg_gen/kg_gen.py:184` to `src/kg_gen/kg_gen.py:203`

### 3.2 Per-Call Model Override

`generate()` can override `model`, `temperature`, `api_key`, `api_base`, `reasoning_effort`.
If any are supplied, it re-runs `init_model()` first.

Reference:
- `src/kg_gen/kg_gen.py:205`

### 3.3 Extraction Worker `_process()`

`_process()` runs both extraction steps under `dspy.context(lm=lm)`:
- `get_entities(...)`
- `get_relations(...)`

Reference:
- `src/kg_gen/kg_gen.py:215`

## 4) Entity Extraction Step

File:
- `src/kg_gen/steps/_1_get_entities.py`

Two execution modes:

1. DSPy mode (default)
- Uses `dspy.Predict(TextEntities)` or `dspy.Predict(ConversationEntities)`.
- Reference: `src/kg_gen/steps/_1_get_entities.py:103`

2. LiteLLM JSON-schema mode
- Triggered when `use_litellm_prompt=True` and not conversation.
- Loads prompt file `prompts/entities.txt`.
- Calls `litellm.responses(...)` with strict JSON schema.
- Reference:
  - `src/kg_gen/steps/_1_get_entities.py:37`
  - `src/kg_gen/steps/_1_get_entities.py:94`

## 5) Relation Extraction Step

File:
- `src/kg_gen/steps/_2_get_relations.py`

### 5.1 Main behavior

Default flow:
- Build relation signature
- ask LLM for list of `{subject, predicate, object}`
- convert to tuples

Reference:
- `src/kg_gen/steps/_2_get_relations.py:223`

### 5.2 LiteLLM strict-schema path

When `use_litellm_prompt=True` and non-conversation:
- dynamically constrains subject/object to extracted entities using `Literal[...]`
- uses strict JSON schema
- parses output with robust fallback parser

Reference:
- model creation `src/kg_gen/steps/_2_get_relations.py:79`
- request path `src/kg_gen/steps/_2_get_relations.py:101`
- parser `src/kg_gen/steps/_2_get_relations.py:9`

### 5.3 Fallback repair path

If primary extraction raises exception:
- runs a more permissive extraction signature
- then runs `ChainOfThought` fixer to force subject/object back into entity set
- filters final relations by entity membership

Reference:
- fallback start `src/kg_gen/steps/_2_get_relations.py:261`
- fixer signature `src/kg_gen/steps/_2_get_relations.py:269`

## 6) Chunking and Parallelism

In `generate()`:
- If no explicit `chunk_size`, it tries once unchunked.
- If exception message contains `"context length"`, auto-switches to `chunk_size=16384`.
- With chunking, it splits text using `chunk_text()` and processes chunks concurrently with `ThreadPoolExecutor`.
- Merges chunk outputs into sets.

Reference:
- context-length fallback `src/kg_gen/kg_gen.py:242`
- chunking parallel path `src/kg_gen/kg_gen.py:254`

Chunker implementation:
- sentence-based split via NLTK tokenizer
- sentence too long -> word-based fallback

Reference:
- `src/kg_gen/utils/chunk_text.py:17`

## 7) Deduplication System

Dispatcher:
- `src/kg_gen/steps/_3_deduplicate.py:17` `run_deduplication(...)`

Methods (`DeduplicateMethod`):
- `SEMHASH`: deterministic-ish semantic hash dedup
- `LM_BASED`: embedding + clustering + LLM aliasing
- `FULL`: run SEMHASH first, then LM-based dedup

Reference:
- enum at `src/kg_gen/steps/_3_deduplicate.py:9`

### 7.1 SEMHASH path

File:
- `src/kg_gen/utils/deduplicate.py`

Algorithm:
1. Normalize text (`unicodedata.NFKC`)
2. Singularize token-wise (`inflect`)
3. `SemHash.from_records(...).self_deduplicate(...)`
4. Map canonical keys back to original forms
5. Remap all relations to deduped entity/edge names
6. Merge `entity_metadata` by deduped representative

Reference:
- normalization/singularization `src/kg_gen/utils/deduplicate.py:28`
- semhash call `src/kg_gen/utils/deduplicate.py:68`
- relation remap `src/kg_gen/utils/deduplicate.py:135`
- metadata merge `src/kg_gen/utils/deduplicate.py:177`

Local compatibility patch in this repo:
- This copy now supports both old/new `semhash` result fields:
  - `selected` or `deduplicated`
  - `duplicates` or `filtered`
- Reference:
  - `src/kg_gen/utils/deduplicate.py:71`
  - `src/kg_gen/utils/deduplicate.py:81`

### 7.2 LM-based dedup path

File:
- `src/kg_gen/utils/llm_deduplicate.py`

High-level:
1. Build embeddings for node/edge strings
2. Build BM25 indices for lexical retrieval
3. KMeans clustering (cluster size target 128)
4. For each item in each cluster:
   - retrieve top relevant candidates (fusion of BM25 + cosine sim)
   - ask LLM for duplicates + alias
5. Build representative sets (`entity_clusters`, `edge_clusters`)
6. Rewrite all relations to representative names
7. Merge metadata keys

Reference:
- embeddings/BM25 init `src/kg_gen/utils/llm_deduplicate.py:37`
- score fusion retrieval `src/kg_gen/utils/llm_deduplicate.py:57`
- clustering `src/kg_gen/utils/llm_deduplicate.py:85`
- per-cluster LLM dedup `src/kg_gen/utils/llm_deduplicate.py:170`
- relation rewrite `src/kg_gen/utils/llm_deduplicate.py:308`

Concurrency:
- cluster dedup futures run with `ThreadPoolExecutor(max_workers=64)` (`src/kg_gen/utils/llm_deduplicate.py:268`).

Important requirement:
- `LM_BASED` and `FULL` require `retrieval_model`, otherwise error.
- Reference: `src/kg_gen/steps/_3_deduplicate.py:24`

## 8) Aggregation, Export, Retrieval

Aggregation:
- `KGGen.aggregate(graphs)` unions entities/relations/edges and merges metadata.
- `src/kg_gen/kg_gen.py:320`

Export:
- `KGGen.export_graph(...)` writes JSON with clusters and metadata.
- `src/kg_gen/kg_gen.py:450`

Retrieval helpers for RAG-like usage:
- `to_nx()` converts to `networkx.DiGraph` (`src/kg_gen/kg_gen.py:362`)
- `generate_embeddings()` for nodes/relations (`src/kg_gen/kg_gen.py:373`)
- `retrieve()` gathers top-k nodes + neighborhood context text (`src/kg_gen/kg_gen.py:390`)

## 9) Visualization and Neo4j Integration

Visualization:
- `src/kg_gen/utils/visualize_kg.py`
- builds rich view model with:
  - deterministic colors
  - degree stats
  - connected components
  - cluster-aware node/edge metadata
- writes HTML by injecting JSON into `template.html`

Reference:
- view model build `src/kg_gen/utils/visualize_kg.py:30`
- render/write `src/kg_gen/utils/visualize_kg.py:264`

Neo4j:
- `src/kg_gen/utils/neo4j_integration.py`
- uploads nodes as `(:Entity {name})`
- converts predicates to relationship types by uppercasing and replacing spaces/hyphens with underscores
- keeps original predicate in `r.predicate`

Reference:
- uploader class `src/kg_gen/utils/neo4j_integration.py:16`
- relationship creation `src/kg_gen/utils/neo4j_integration.py:140`

## 10) Real-World Caveats You Should Know

1. API drift in docs/tests vs code:
- README and one test still show `cluster=True` in `generate(...)`.
- Current signature is `deduplication_method=...` instead.
- References:
  - current signature `src/kg_gen/kg_gen.py:155`
  - old README usage `README.md:93`, `README.md:197`
  - old test usage `tests/test_chunked.py:57`

2. `no_dspy=True` does not mean "no DSPy everywhere":
- It switches extraction to LiteLLM prompt path only for non-conversation extraction.
- Dedup may still use DSPy if `LM_BASED`/`FULL`.
- References:
  - entity gate `src/kg_gen/steps/_1_get_entities.py:94`
  - relation gate `src/kg_gen/steps/_2_get_relations.py:237`

3. `get_relations()` filter comment mismatch:
- function says filter backslashes, code filters double quotes (`"`).
- reference: `src/kg_gen/steps/_2_get_relations.py:218`

4. Embedding generation has a TODO about index issues:
- reference: `src/kg_gen/kg_gen.py:385`

## 11) Recommended Extension Points for Your Custom Fork

If you plan to iterate this with your own project, these are the safest hooks:

1. Prompt control:
- edit `src/kg_gen/prompts/entities.txt`
- edit `src/kg_gen/prompts/relations.txt`

2. Post-processing policy:
- add custom relation filters in `src/kg_gen/steps/_2_get_relations.py` after extraction/fix stage.

3. Dedup policy:
- adjust semhash threshold in `KGGen.deduplicate(..., semhash_similarity_threshold=...)`
- or add your own method by extending `DeduplicateMethod` and `run_deduplication()`.

4. Domain metadata:
- enrich `Graph.entity_metadata` and keep propagation behavior in dedup modules.

5. Runtime/observability:
- `extract_token_usage_from_history()` in `src/kg_gen/kg_gen.py:472` is a simple place to attach cost monitoring.

