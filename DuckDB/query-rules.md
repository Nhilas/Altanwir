# Query rules — Altanwir Gold (DuckDB)

**Agent: read this file before composing queries against `gold.*` views. The patterns below are not suggestions; ignoring them produces wrong results or 30-50 hour query times.**

This file presumes the domain vocabulary in [`agent-orientation-primer.md`](agent-orientation-primer.md). If a term in a rule below is unfamiliar (`gameKey`, `factreviews`, `vw_gameCatalogue`, etc.), read the primer's §2 Glossary first.

Two rules. Both are mandatory.

---

## Rule 1 — `gameKey`-first query pattern

### Trap

`gold.factreviews` is ~71M rows. The Gold serving views (`vw_factGameScores`, `vw_factReviews`, the agg views, `vw_gameCatalogue`) join through it. A query that filters on a human-readable attribute (e.g. `gameName = '...'`, `genreName = '...'`) without first resolving to a `gameKey` set will scan the full view chain.

### Rule

Resolve to a small `gameKey` set first via the cheap dimension views (`vw_dimGames`, `vw_gameGenres`, `vw_gameThemes`, `vw_gamePlatforms`, or `vw_factGameScores` filtered on game-grain attributes). Then filter `gold.factreviews` (or any view joining through it) with `gameKey IN (<small-set>)`. For sets of ≤20 games the review-grain query runs in ~2 minutes against the parquet exports.

`gameKey` is the SHA-256 surrogate key over the IGDB game id. It is the only filter `gold.factreviews` is realistically pruneable on, given the file layout. Names and other dim attributes only work cheaply on the game-grain side.

### Example

```sql
-- BAD — scans the full view chain. 30-50 hours.
SELECT gameName, COUNT(*) AS reviews
FROM gold.vw_factReviews
WHERE gameName IN ('DOOM', 'Starfield', 'Disco Elysium')
GROUP BY gameName;

-- GOOD — resolve gameKey set on the cheap side, then filter factreviews.
WITH target_games AS (
    SELECT gameKey
    FROM gold.vw_dimGames
    WHERE gameName IN ('DOOM', 'Starfield', 'Disco Elysium')
)
SELECT g.gameName, COUNT(*) AS reviews
FROM gold.factreviews f
JOIN target_games t ON f.gameKey = t.gameKey
JOIN gold.vw_dimGames g ON f.gameKey = g.gameKey
GROUP BY g.gameName;
```

The cheap side is the dimension join. The expensive side is the 71M-row scan. The rule puts the filter on the expensive side.

---

## Rule 2 — `vw_gameCatalogue` cartesian dedup trap

### Trap

`vw_gameCatalogue` is a flat catalog of game × genre × theme × platform. A game with 3 genres, 4 themes, and 5 platforms produces 3 × 4 × 5 = 60 rows in the catalog. A `ROW_NUMBER() OVER (ORDER BY <score> DESC)` applied directly to this view will rank that same game 1, 2, 3, ..., 60 — each multi-attribute row picks up its own rank. Top-N queries against the catalog without dedup return the same handful of games repeated, not the top N distinct games.

### Rule

Dedup to `gameKey` grain in a CTE before applying any `ROW_NUMBER()`, `RANK()`, `LIMIT N`, or window-function rank. `SELECT DISTINCT gameKey` plus the columns being ranked is enough. Apply the window function on the dedupped CTE, then join back to `vw_gameCatalogue` (or `vw_factGameScores`) if multi-attribute display columns are needed.

### Example

```sql
-- BAD — same game ranks 1/2/3/.../60 across its genre × theme × platform tags.
SELECT
    gameName,
    weightedSentimentRating,
    ROW_NUMBER() OVER (ORDER BY weightedSentimentRating DESC) AS rnk
FROM gold.vw_gameCatalogue
WHERE totalReviews >= 1000;

-- GOOD — dedup to gameKey grain first, then rank.
WITH dedupped AS (
    SELECT DISTINCT
        gameKey,
        gameName,
        weightedSentimentRating,
        totalReviews
    FROM gold.vw_gameCatalogue
    WHERE totalReviews >= 1000
),
ranked AS (
    SELECT
         gameKey
        ,gameName
        ,weightedSentimentRating
        ,ROW_NUMBER() OVER (ORDER BY weightedSentimentRating DESC) AS rnk
    FROM dedupped
)
SELECT * FROM ranked WHERE rnk <= 25;
```

When the report needs the multi-attribute display (genres, themes, platforms per game), do the rank on dedupped CTE first, then join back to `vw_gameCatalogue` to expand the chosen `gameKey`s.
