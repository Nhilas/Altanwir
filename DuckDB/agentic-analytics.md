# Agentic analytics over the Altanwir lakehouse

## What it is

The Altanwir Gold layer was queried at portfolio time not by a human writing SQL directly, but by an agent collaborating with a human reviewer. The pattern: a well-structured repo (Bronze → Silver → Gold, dimensional, ADR-documented), a domain canon scoped to the column meanings the agent had to know, a strict query-rules document the agent had to follow, and verification sub-agents tasked with auditing findings before they shipped. The agent wrote the queries, surfaced patterns, and drafted the findings; the human reviewer ran the loop end-to-end and decided what to ship.

The transition from Fabric to DuckDB is the structural reason this layer exists at all. Fabric covered the production pipeline through trial expiry; with cluster capacity gone, the Gold parquet exports were loaded into a local DuckDB harness to keep ad-hoc analytics running. The agentic loop runs against that harness, with three artifacts as scaffolding: a domain canon ([`agent-orientation-primer.md`](agent-orientation-primer.md)), this methodology, and a query-rules doc ([`query-rules.md`](query-rules.md)).

## The loop

Four components:

**The repo.** A medallion lakehouse with clean separation between schema-resilient Bronze, scored Silver, dimensional Gold base tables, and a 13-view Gold serving layer. The architecture is documented in [`../Docs/architecture/overview.md`](../Docs/architecture/overview.md); the per-game scoring math in [`../Docs/architecture/scoring-model.md`](../Docs/architecture/scoring-model.md).

**The agent canon.** The orientation primer defines every column the agent had access to: naming conventions (`signal` means 0-1 per-review, `rating` means smoothed game-grain), the per-convention YAML contracts, exceptions where a column breaks its convention, and a full term-by-term glossary. This is the canon the agent reads before composing a query. It is **Exhibit A**: without it, the patterns below do not reproduce.

**Query rules.** A separate file holds prescriptive must-follow patterns — the `gameKey`-first query pattern (query efficiency), and the `vw_gameCatalogue` cartesian dedup trap. These are not embedded in the methodology. They live next to it, scoped to agent consumption, so they can evolve as new traps surface without rewriting the prose.

**Verification sub-agents.** Drafts are audited before they ship. A verification sub-agent re-reads each draft against the primer's term definitions and the underlying parquet data — checking that column meanings line up, that joins have not introduced multi-counts, and that any cited number reproduces from the harness. On 2026-05-10, this audit caught real semantic drift across the findings layer: a term used inconsistently across multiple docs. Findings were corrected before they were filed as the durable analytical artifacts in [`../Docs/findings/`](../Docs/findings/).

The six findings the loop produced, each cited here by name:

- [`edge-cases.md`](../Docs/findings/edge-cases.md) — the games where weighted sentiment and weighted vote rating diverge most sharply.
- [`funny.md`](../Docs/findings/funny.md) — joke-review patterns and how they survive the scoring layer.
- [`protest-reviews.md`](../Docs/findings/protest-reviews.md) — what coordinated review-bombing looks like in the influence-weighted output.
- [`sentiment-vote-alignment.md`](../Docs/findings/sentiment-vote-alignment.md) — the per-game gap between text-sentiment and recommend-vote ratings.
- [`what-sentimentrating-reveals.md`](../Docs/findings/what-sentimentrating-reveals.md) — what the smoothed text-sentiment leaderboard surfaces about audience taste.
- [`where-the-gap-grows.md`](../Docs/findings/where-the-gap-grows.md) — how the sentiment-vote gap scales with audience size.

## Key framings

**Visible flow.** Every step of the loop is committed: the repo, the canon, the rules, the findings. Findings can be traced back through the primer's term definitions to the underlying parquet column.

**Collaboration without losing rigor.** The agent traverses the canon faster than a human reading raw view DDLs and follows the query rules without forgetting them mid-session. It does not judge which finding is worth shipping, decide framing for a non-technical audience, or catch a misread of intent. The verification sub-agents close part of the loop; the human reviewer closes the rest.

**Reusable scaffolding.** Any structured lakehouse with column-meaning drift across docs is a candidate for the same pattern — a domain canon, prescriptive query rules, a verification step. The loop scales to larger surfaces by carving the canon into per-domain slices.

## References

- [`agent-orientation-primer.md`](agent-orientation-primer.md) — domain canon: column meanings, morpheme decoder, architecture orientation.
- [`query-rules.md`](query-rules.md) — must-follow query patterns against the `gold.*` views.
- [`README.md`](README.md) — DuckDB harness setup and what the database contains.
- [`../Docs/findings/`](../Docs/findings/) — the six findings the loop produced.
- [`../Docs/architecture/overview.md`](../Docs/architecture/overview.md) — full pipeline architecture.
- [`../Docs/architecture/scoring-model.md`](../Docs/architecture/scoring-model.md) — scoring math and shrinkage formulas.
