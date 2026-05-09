# Altanwir

> Steam reviews × IGDB metadata · Medallion lakehouse on Microsoft Fabric · Bronze → Silver → Gold → Views.

## Architecture

<a href="Docs/architecture/diagrams/architecture-summary.png"><img src="Docs/architecture/diagrams/architecture-summary.png" alt="Medallion pipeline summary — sources to serving views"></a>

*Full pipeline + legend: [`Docs/architecture/overview.md#diagram`](Docs/architecture/overview.md#diagram).*

---

See [`Docs/architecture/overview.md`](Docs/architecture/overview.md) for the full architectural writeup, including layer contracts, the data model, engineering patterns with ADR links, and the run-time profile (71M reviews end-to-end in ~2h 28m on an 8-core trial cluster).
