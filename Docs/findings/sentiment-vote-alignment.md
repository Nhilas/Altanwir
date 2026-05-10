# Sentiment-vote alignment

Steam reviews carry two signals at once: the recommend / not-recommend thumb (the **vote**), and the text the player wrote. A sentiment scorer called VADER reads each review's text and returns a number from -1 (very negative) to +1 (very positive).

The pipeline aggregates those per-review scores up to the game level, rescales the result to a 0-100 range, and applies a smoothing step called *shrinkage* that nudges games with very few reviews toward the dataset average. So a 100%-positive 5-review indie doesn't outrank a 95%-positive 50,000-review behemoth. (Full method: [scoring-model.md](../architecture/scoring-model.md).) The vote signal goes through the same treatment. Both end up on a 0-100 scale, and the **alignment** metric is their difference, in percentage points:

```text
sentimentVoteAlignment = weightedSentimentRating - weightedVoteRating
```

In the tables below, these are shortened to *text sentiment* and *vote rating*. Negative means players write angrier text than they vote. Positive means players write milder text than they vote. Most games sit within a few points of zero. The tails are where it gets interesting.

| Game | text sentiment | vote rating | Steam label | alignment | reads as |
|---|---:|---:|---|---:|---|
| Stardew Valley | 93.3 | 98.5 | Overwhelmingly Positive | -5.2 | both ratings near the ceiling, gap stays small |
| Phasmophobia | 79.4 | 95.6 | Very Positive | -16.3 | rage-positive horror |
| Sekiro: Shadows Die Twice | 76.9 | 94.1 | Very Positive | -17.2 | frustration-love (Souls-like) |
| Starfield | 73.5 | 56.9 | Mixed | +16.6 | text milder than the down-votes suggest |
| Outlast | 67.8 | 95.4 | Very Positive | -27.5 | extreme rage-positive (horror) |

*Source: ad-hoc DuckDB lookup against `gold.vw_factGameScores`.*

> **Column shorthand used in the tables below:** *avg playtime (h)* is average hours played when the review was written. *% negative-text reviews* is the share of a game's reviews where VADER scored the text clearly negative (compound score ≤ -0.05). *% early-access reviews* is the share written while the game was sold-but-still-in-development (Steam's *Early Access* tag). *% bug mentions* is the share of reviews mentioning *bug, crash, error, lag, stuck,* or *glitch*. *% refunds* is the share of reviews written by accounts that refunded the game. *Genres / themes* come from IGDB.

## Negative tail: where players write angrier text than they vote

Recognizable IPs with at least 50,000 reviews.

*Source: `Q01-alignment-negative.sql`*

| Game | alignment | review count | Steam label | avg playtime (h) | % negative-text reviews | % early-access reviews | % bug mentions | % refunds | genres / themes |
|---|---:|---:|---|---:|---:|---:|---:|---:|---|
| Doom | -21.16 | 117,016 | Very Positive | 24.7 | 23.83 | 0% | 2.93 | 0.24 | Shooter / Action, Horror, Sci-fi, Survival |
| Doom Eternal | -18.31 | 132,442 | Very Positive | 29.6 | 24.91 | 0% | 2.86 | 0.27 | Shooter / Action, Fantasy, Horror, Sci-fi, Warfare |
| Sekiro: Shadows Die Twice | -17.23 | 97,778 | Very Positive | 49.0 | 21.56 | 0% | 2.02 | 0.44 | Adventure / Action, Fantasy, Historical, Stealth |
| Phasmophobia | -16.27 | 426,001 | Very Positive | 39.6 | 19.03 | 100% | 3.23 | 0.19 | Indie, Puzzle, Tactical / Action, Horror, Thriller |
| Lethal Company | -15.64 | 340,226 | Overwhelmingly Positive | 24.7 | 16.56 | 100% | 2.33 | 0.35 | Indie / Action, Comedy, Horror, Sci-fi |
| Project Zomboid | -14.84 | 190,903 | Very Positive | 132.6 | 19.05 | 100% | 2.75 | 0.44 | Indie, RPG, Simulator / Horror, Open world, Sandbox, Survival |
| RimWorld | -14.13 | 137,590 | Overwhelmingly Positive | 410.4 | 15.57 | 10.1% | 2.42 | 0.12 | Indie, RTS, Simulator, Strategy / Sci-fi, Survival |
| Dark Souls III | -13.76 | 164,124 | Very Positive | 110.2 | 18.22 | 0% | 2.85 | 0.28 | Adventure, RPG / Action, Fantasy |
| Cuphead | -13.39 | 70,104 | Very Positive | 28.7 | 15.99 | 0% | 2.20 | 0.42 | Adventure, Arcade, Indie, Platform, Shooter / Action, Comedy, Fantasy |
| Dead Cells | -13.37 | 50,002 | Very Positive | 44.2 | 15.75 | 12.5% | 2.04 | 0.23 | Adventure, Indie, Platform / Action, Fantasy |

Two informal clusters jump out (a read of the games, not separate measured columns). The first reads as **punishing-difficulty**: Doom, Sekiro, Souls, Cuphead, Dead Cells. Players write angry-sounding reviews about dying repeatedly, then click recommend. The second reads as **survival-horror sandbox**: Phasmophobia, Lethal Company, Project Zomboid, RimWorld. Players write frustrated text about scares and colony losses, then recommend the game anyway. Three of the survival-horror set are 100% early access. RimWorld at 410 hours of average playtime is the deep-investment marker. These are veterans, not drive-by gripers.

## Positive tail: where players write milder text than they vote

*Source: `Q02-alignment-positive.sql`*

| Game | alignment | review count | Steam label | avg playtime (h) | % negative-text reviews | % bug mentions | % early-access reviews | genres / themes |
|---|---:|---:|---|---:|---:|---:|---:|---|
| Starfield | +16.64 | 115,564 | Mixed | 85.9 | 24.08 | 15.30 | 0% | Adventure, RPG, Shooter / Action, Open world, Sandbox, Sci-fi |
| Borderlands 4 | +11.87 | 54,350 | Mixed | 36.2 | 22.97 | 22.73 | 0% | Adventure, RPG, Shooter / Action, Sci-fi |
| Ark: Survival Ascended | +10.67 | 57,523 | Mixed | 173.1 | 24.29 | 26.01 | 100% | Adventure, Indie, RPG, Shooter, Simulator / Action, Open world, Sandbox, Sci-fi, Survival |
| Battlefield 2042 | +10.39 | 132,589 | Mixed | 45.0 | 35.63 | 16.18 | 0% | Shooter / Action, Warfare |
| Lost Ark | +9.87 | 104,658 | Mixed | 275.1 | 19.22 | 3.33 | 0% | Adventure, Hack and slash, RPG / Action, Fantasy |
| Monster Hunter Wilds | +8.86 | 115,183 | Mixed | 70.5 | 21.39 | 14.56 | 0% | Adventure, Hack and slash, RPG / Action, Fantasy, Open world |
| Apex Legends | +7.07 | 60,607 | Mixed | 446.0 | 34.38 | 3.90 | 0% | Shooter, Tactical / Action, Sci-fi |

Reads as **disappointment-AAA** (AAA = big-budget franchise launches). High-volume launches, "Mixed" Steam labels, bug-mention rates between 14-26%. Players write "I love this franchise, but..." text and click not-recommended. Apex Legends at 446 hours of average playtime is the cluster's deep-investment signal: the people writing these reviews are not casual. They have hundreds of hours in and are still upset enough to thumbs-down, but reasonable enough that VADER scores their text positive.

## The pattern is structural

The same shape shows up at two other levels of the data:

- **Across all horror games**: 2,506 games and 11.1M reviews. Average alignment is **-9.23**. Thriller sits at -8.21, Survival at -6.26. Same direction as Phasmophobia at the per-game level, just averaged across the whole theme. *(Source: `Q07-theme-alignment.sql`)*
- **Across the most-reviewed games**: the top 10 by review count (CS:GO, Dota 2, Helldivers 2, Rainbow Six Siege, TF2, Terraria, GTA V, Rust, Garry's Mod, Elden Ring) all have negative alignment, ranging from -3 to -12. At popularity scale, vote optimism reliably exceeds sentiment optimism (in plainer terms: the thumbs are kinder than the words). Rage-positive is not a horror-game quirk; it is what scale does to votes. *(Source: `Q13-popularity.sql`)*

## Other notable extremes

Games with at least 1,000 reviews, ordered by absolute alignment.

*Source: `Q14-most-divisive.sql`*

| Game | alignment | review count | Steam label | avg playtime (h) | % negative-text reviews | % bug mentions | % early-access reviews | genres / themes |
|---|---:|---:|---|---:|---:|---:|---:|---|
| Fear & Hunger | -34.24 | 11,480 | Very Positive | 17.6 | 38.82 | 5.63 | 0% | Indie, RPG, TBS / Fantasy, Horror, Survival |
| Ratshaker | -32.44 | 2,889 | Very Positive | 1.9 | 40.91 | 1.04 | 0% | Adventure, Indie / Action, Horror, Mystery |
| Godus | +30.36 | 5,457 | Mostly Negative | 31.2 | 37.68 | 9.22 | 100% | Indie, Point-and-click, RTS, Simulator, Strategy / 4X, Open world, Sandbox |
| RollerCoaster Tycoon World | +29.68 | 3,184 | Mostly Negative | 18.0 | 34.70 | 37.91 | 39.0% | Simulator, Strategy / Business, Sandbox |
| Amnesia: The Dark Descent | -27.68 | 20,723 | Very Positive | 14.7 | 31.23 | 2.22 | 0% | Adventure, Indie, Puzzle / Action, Horror, Survival |
| Outlast | -27.54 | 38,799 | Very Positive | 10.4 | 29.78 | 1.98 | 0% | Adventure, Indie / Action, Horror, Mystery, Stealth, Survival |

Outlast at -27.54 is the most-recognizable rage-positive extreme. Godus at +30.36 (Peter Molyneux's flop) is the most-recognizable disappointment extreme. Pair them and the metric reads at its sharpest.

## Sources

- Queries: `Q01-alignment-negative.sql`, `Q02-alignment-positive.sql`, `Q07-theme-alignment.sql`, `Q13-popularity.sql`, `Q14-most-divisive.sql` (battery preserved alongside this finding)
- Reproduce via: [Labs/Lab03_duckdb_gold/](../../Labs/Lab03_duckdb_gold/) (DuckDB harness over Gold parquet exports)
- Methodology: [scoring-model.md](../architecture/scoring-model.md) (§ `sentimentVoteAlignment`)
- Companion finding: [score-distributions.md](score-distributions.md) (volume-bucket detail behind why the gap shows up where it does)
- Caveats:
  - Both ratings are smoothed (the *shrinkage* step mentioned in the lede; technical name is empirical-Bayes shrinkage), so games with very few reviews don't generate misleading alignment values. Formula and priors live in `scoring-model.md`.
  - The recognizable-IP scoping for the ≥50k tables is curatorial (drawn from a list of broadly familiar games to keep the prose readable), not statistical. The "Other notable extremes" section uses ≥1,000 reviews and no name filter.
  - Genres / themes pulled from `gold.vw_gameCatalogue`. Some IGDB labels abbreviated for table width: "Role-playing (RPG)" → RPG, "Real Time Strategy (RTS)" → RTS, "Turn-based strategy (TBS)" → TBS, "Hack and slash/Beat 'em up" → Hack and slash, "Science fiction" → Sci-fi, "4X (explore, expand, exploit, and exterminate)" → 4X. "Unknown" entries (IGDB placeholder) stripped.
