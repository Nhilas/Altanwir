# Fabric references

_Last updated: 2026-05-04_

## Spark catalog and SQL analytics endpoint are separate

Fabric Lakehouse has two catalogs that share _tables_ but not _views_:

- **Spark catalog.** Where `spark.sql("CREATE VIEW ...")` registers views. Visible to Spark notebooks only.
- **SQL analytics endpoint.** Auto-exposes Delta tables from the Lakehouse. Does NOT see Spark views.

**Implication:** A view created via Spark SQL in a notebook is invisible to the SQL analytics endpoint. "Invalid object name" error when querying.

**Workarounds:**

- Create the view directly in the SQL analytics endpoint using T-SQL (different editor in Fabric UI).
- OR materialize as a Delta table (both catalogs see tables).
- OR accept duplication: define the view in both places.

**How to apply:** When designing Gold serving layer views, ask upfront whether the consumer is Spark notebooks or the SQL endpoint. If both, pick materialization or document the split.

---

## SHOW CREATE TABLE not supported on Delta tables

`SHOW CREATE TABLE` throws `DELTA_OPERATION_NOT_ALLOWED` on Delta tables in Fabric.

**Alternatives for extracting schema:**

```python
# Readable column list
spark.sql("DESCRIBE TABLE tablename").show(100, truncate=False)

# Generate CREATE TABLE DDL from the DataFrame schema
schema = spark.table("tablename").schema
cols = ",\n  ".join([f"`{f.name}` {f.dataType.simpleString()}" for f in schema.fields])
print(f"CREATE TABLE name (\n  {cols}\n)")
```

---

## Spark views need SHOW VIEWS IN <schema> to locate

Views created via Spark SQL don't always appear in the Fabric Lakehouse UI's explorer next to tables. To confirm a view exists:

```python
spark.sql("SHOW VIEWS IN gold").show(truncate=False)
```

Returns namespace (GUID-based), viewName, isTemporary.

---

## `CLUSTER BY` must precede `TBLPROPERTIES` in CREATE TABLE DDL

**What:** When creating a liquid-clustered Delta table, the `CLUSTER BY (col)` clause must appear before `TBLPROPERTIES` in the DDL statement. Reversing the order causes a Spark SQL parse error.

**Why it bites:** Most Delta Lake examples put `TBLPROPERTIES` first (for `delta.enableChangeDataFeed`, `delta.autoOptimize`, etc.). Adding `CLUSTER BY` after `TBLPROPERTIES` fails silently in some editors and loudly in others. The error message does not mention ordering.

**What to do:** Structure DDL as `CREATE TABLE ... CLUSTER BY (col) TBLPROPERTIES (...)`. Not `CREATE TABLE ... TBLPROPERTIES (...) CLUSTER BY (col)`.

---

## `ALTER TABLE ... CLUSTER BY` is unsupported in Fabric

**What:** Liquid cluster keys cannot be changed after table creation in Microsoft Fabric. `ALTER TABLE ... CLUSTER BY (newCol)` is not supported. Only `CREATE TABLE ... CLUSTER BY` works at DDL time.

**Why it bites:** If the cluster key choice turns out to be wrong (e.g., Bronze was initially clustered on `app_id` for an ingest that merges on `recommendationid`), the only fix is to drop and recreate the table. This was discovered during the Apr 23 production deploy when Bronze and Silver cluster keys needed correction.

**What to do:** Treat cluster-key choice as a commitment at create time. If a key change is needed, plan a full table drop + recreate + reload cycle. Document the chosen cluster key and its rationale (which predicate it serves) so future maintainers understand why it was chosen.

---

## Quick reference

- **Spark views ≠ SQL endpoint views.** Separate catalogs. Views created via `spark.sql()`
  are invisible to the SQL analytics endpoint. Need to define T-SQL view in the endpoint
  directly for demo queries.
- **`SHOW CREATE TABLE` blocked on Delta.** Use `DESCRIBE TABLE` or DataFrame schema
  introspection instead.
- **Spark views don't show in Lakehouse UI.** Use `SHOW VIEWS IN <schema>` to find them.
- **Complex types blocked in Delta via SQL endpoint.** No arrays, structs, maps. This is
  why Gold uses flat strings and bridge tables instead of `collect_list`. See [ADR-001](../adrs/adr-001-dimensional-gold-over-array-obt.md).
- **`targeted` load type has a data trap.** Percentile windows run over filtered subset,
  not full population. Safe for testing merge mechanics only; not production data. See [ADR-004](../adrs/adr-004-percentiles-in-views.md).
