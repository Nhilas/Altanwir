---
name: Fabric platform gotchas
description: Known Microsoft Fabric Lakehouse limitations and workarounds discovered during Altanwir build — split Spark/SQL catalogs, Delta DDL restrictions
type: reference
originSessionId: 96b9a1a2-2602-445b-8de0-07bda3f0c54e
---
## Spark catalog and SQL analytics endpoint are separate

Fabric Lakehouse has two catalogs that share *tables* but not *views*:

- **Spark catalog** — where `spark.sql("CREATE VIEW ...")` registers views. Visible to Spark notebooks only.
- **SQL analytics endpoint** — auto-exposes Delta tables from the Lakehouse. Does NOT see Spark views.

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

## More

- **Spark views ≠ SQL endpoint views.** Separate catalogs. Views created via `spark.sql()`
  are invisible to the SQL analytics endpoint. Need to define T-SQL view in the endpoint
  directly for demo queries.
- **`SHOW CREATE TABLE` blocked on Delta** — use `DESCRIBE TABLE` or DataFrame schema
  introspection instead.
- **Spark views don't show in Lakehouse UI** — use `SHOW VIEWS IN <schema>` to find them.
- **Complex types blocked in Delta via SQL endpoint** — no arrays, structs, maps. This is
  why Gold uses flat strings and bridge tables instead of `collect_list`. See ADR_003.
- **`targeted` load type has a data trap** — percentile windows run over filtered subset,
  not full population. Safe for testing merge mechanics only; not production data.