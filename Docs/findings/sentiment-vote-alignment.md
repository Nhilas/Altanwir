# Sentiment-vote alignment

## What this doc covers

Steam reviews come with a thumb and a paragraph, and those two often disagree. The pipeline scores each review's words with VADER (a sentiment scorer that returns a number from -1 for very negative to +1 for very positive), aggregates per-review scores up to the game level, rescales to a 0-100 range, and applies a smoothing step called *shrinkage* that pulls low-volume games toward the dataset average so a 5-review indie does not outrank a 50,000-review behemoth. The vote signal is processed the same way. The **alignment** metric is the difference between the two on the 0-100 scale: positive means the words are milder than the votes, negative means the words are angrier than the votes.

This doc walks the negative tail of alignment at game grain (where players write angrier text than they vote), then the positive tail (where players write milder text than they vote), then asks whether the same shape shows up at theme and popularity grain. It does not restate the smoothing formulas or priors (those live in [scoring-model.md](../architecture/scoring-model.md)).

The single sentence the doc makes: most games sit within a few points of zero alignment, and the games that don't fall into recognisable clusters: punishing-difficulty and survival-horror games on the angry-text / positive-vote side, disappointing AAA launches on the mild-text / negative-vote side, and the same shape repeats one level up at theme and popularity grain.

## Glossary

| Term | Plain meaning |
|---|---|
| **VADER** | a sentiment scorer that reads a review's words and returns a number from -1 to +1 |
| **compound score** (`sentimentCompound`) | VADER's output number for a single review, ranging -1 to +1. Above +0.05 reads as positive; below -0.05 reads as negative; in between reads as neutral. |
| **text sentiment rating** (`weightedSentimentRating`) | per-game 0-100 score. Influence-weighted average of per-review sentiment directions (positive or negative, derived from each review's VADER compound score) across VADER-eligible reviews, then smoothed. Shortened to **text sentiment** in the tables. |
| **vote rating** (`weightedVoteRating`) | per-game 0-100 score. Influence-weighted average of every thumb (yes / no), then smoothed. |
| **alignment** (`sentimentVoteAlignment`) | text sentiment minus vote rating, in percentage points. Negative = angrier text than vote. Positive = milder text than vote. |
| **influence score** (`reviewInfluenceScore`) | a 0-1 weight per review. Blends five signal columns on each review row: `communitySignal`, `playtimeSignal`, `lengthSignal`, `emotionalSignal`, `sentimentSignal`. `communitySignal` is itself a sub-blend of helpful / funny / comment / reaction votes (weights 0.45 / 0.20 / 0.25 / 0.10). |
| **smoothing** (empirical-Bayes shrinkage) | a step that nudges low-volume games toward the dataset average. Formula and priors in `scoring-model.md`. **Post-shrinkage** = the value after this step is applied; it's the version stored on `vw_factGameScores` and used in every table on this page. |
| **% negative-text reviews** (`pctNegativeSentiment`) | share of a game's reviews where VADER scored the text clearly negative (compound score ≤ -0.05) |
| **% early-access reviews** (`pctEarlyAccess`) | share of a game's reviews written while the game was sold-but-still-in-development (Steam's *Early Access* tag) |
| **% bug mentions** (`pctBugReports`) | share of reviews mentioning *bug, crash, error, lag, stuck,* or *glitch* |
| **% refunds** (`pctRefunded`) | share of a game's reviews written by accounts that refunded the game |
| **avg playtime (h)** (`avgPlaytimeAtReviewHours`) | average hours played at the time the review was written |
| **Steam label** (`steamRatingLabel`) | Steam's own bucket: Overwhelmingly Positive, Very Positive, Mostly Positive, Mixed, Mostly Negative, Overwhelmingly Negative. Buckets depend on review count (see `semantic-layer-lite.md` §4). |
| **AAA** | big-budget franchise launches |
| **EA** | early access (used as shorthand in the EA-volatility paragraphs) |

## Contents

- [What alignment looks like](#what-alignment-looks-like)
- [Where players write angrier text than they vote](#where-players-write-angrier-text-than-they-vote)
  - [The reviews behind the angry-text picks](#the-reviews-behind-the-angry-text-picks)
- [Where players write milder text than they vote](#where-players-write-milder-text-than-they-vote)
  - [The reviews behind the mild-text picks](#the-reviews-behind-the-mild-text-picks)
- [Refunds concentrate in the early-access shooter cluster](#refunds-concentrate-in-the-early-access-shooter-cluster)
- [The pattern is structural](#the-pattern-is-structural)
- [Other notable extremes](#other-notable-extremes)
- [Sources](#sources)

## What alignment looks like

One game per bucket across the alignment range, ordered most-positive to most-negative.

*Source: `Q29-alignment-explainer.sql`*

| Game | text sentiment | vote rating | Steam label | alignment | reads as |
|---|---:|---:|---|---:|---|
| Starfield | 73.54 | 56.90 | Mixed | +16.64 | text milder than the down-votes suggest |
| Stardew Valley | 93.27 | 98.51 | Overwhelmingly Positive | -5.24 | both ratings near the ceiling, gap stays small |
| Phasmophobia | 79.36 | 95.63 | Very Positive | -16.27 | rage-positive horror |
| Sekiro: Shadows Die Twice | 76.89 | 94.12 | Very Positive | -17.23 | frustration-love (Souls-like) |
| Outlast | 67.84 | 95.38 | Very Positive | -27.54 | extreme rage-positive (horror) |

Most games sit within a few points of zero. The two tails below show what the larger gaps look like.

## Where players write angrier text than they vote

Recognisable IPs with at least 50,000 reviews, ordered by alignment ascending.

*Source: `Q01-alignment-negative.sql`*

| Game | alignment | review count | Steam label | avg playtime (h) | % negative-text reviews | % early-access reviews | % bug mentions | % refunds | genres / themes |
|---|---:|---:|---|---:|---:|---:|---:|---:|---|
| Doom | -21.16 | 117,016 | Very Positive | 24.7 | 23.83 | 0% | 2.93 | 0.24 | Shooter / Action, Horror, Sci-fi |
| Doom Eternal | -18.31 | 132,442 | Very Positive | 29.6 | 24.91 | 0% | 2.86 | 0.27 | Shooter / Action, Fantasy, Horror, Sci-fi, Warfare |
| Sekiro: Shadows Die Twice | -17.23 | 97,778 | Very Positive | 49.0 | 21.56 | 0% | 2.02 | 0.44 | Adventure / Action, Fantasy, Historical, Stealth |
| Phasmophobia | -16.27 | 426,001 | Very Positive | 39.6 | 19.03 | 100% | 3.23 | 0.19 | Indie, Puzzle, Tactical / Action, Horror, Thriller |
| Lethal Company | -15.64 | 340,226 | Overwhelmingly Positive | 24.7 | 16.56 | 100% | 2.33 | 0.35 | Indie / Action, Comedy, Horror, Sci-fi |
| Project Zomboid | -14.84 | 190,903 | Very Positive | 132.6 | 19.05 | 100% | 2.75 | 0.44 | Indie, RPG, Simulator / Horror, Open world, Sandbox, Survival |
| RimWorld | -14.13 | 137,590 | Overwhelmingly Positive | 410.4 | 15.57 | 10.1% | 2.42 | 0.12 | Indie, RTS, Simulator, Strategy / Sci-fi, Survival |
| Dark Souls III | -13.76 | 164,124 | Very Positive | 110.2 | 18.22 | 0% | 2.85 | 0.28 | Adventure, RPG / Action, Fantasy |
| Cuphead | -13.39 | 70,104 | Very Positive | 28.7 | 15.99 | 0% | 2.20 | 0.42 | Adventure, Arcade, Indie, Platform, Shooter / Action, Comedy, Fantasy |
| Dead Cells | -13.37 | 50,002 | Very Positive | 44.2 | 15.75 | 12.5% | 2.04 | 0.23 | Adventure, Indie, Platform / Action, Fantasy |

Two clusters here, both informal. **Punishing-difficulty** owns six rows: Doom, Doom Eternal, Sekiro, Cuphead, Dead Cells, Dark Souls III. Players type angry sentences about dying twenty times to the same boss, then click recommend. **Survival-horror sandbox** owns the other four, and three of those ran 100% early access. RimWorld at 410 hours of average playtime closes the row; these are veterans typing through gritted teeth, not drive-by gripers.

### The reviews behind the angry-text picks

Top review per game by `reviewInfluenceScore`. Picks span both clusters: Doom and Sekiro for punishing-difficulty, Phasmophobia for survival-horror.

*Source: `Q30-alignment-drilldown.sql`*

| game | influence | upvotes | funny | comments | recommended? | playtime (h) | VADER score (-1 to +1) |
|---|---:|---:|---:|---:|---|---:|---:|
| Doom | 0.754 | 2,217 | 2,215 | 57 | YES | 59.5 | +0.8845 |
| Sekiro: Shadows Die Twice | 0.771 | 665 | 61 | 21 | YES | 216.2 | -0.9758 |
| Phasmophobia | 0.761 | 1,259 | 85 | 25 | YES | 248.4 | +0.9987 |

**Doom** (2,215 funny votes, full review):

> This game has no swearing, no drug use, no objectification of women, no killing of people, no sexually explicit material and no product placement. Just good, wholesome demon-killing fun. Get this game for your children and grandchildren. Edit: it's all jokes dudes, get the irony? dang lol

**Sekiro: Shadows Die Twice** (216.2h at review, VADER -0.9758):

> Because of how fast the game is, no magic or summons, no grinding levels or equipment, no shortcuts compared to other souls games and punishing aggressive spamming gameplay while rewarding engaging in fights while calm I quickly found out I have an inferiority complex with BPD. Sekiro is the reason I figured more about myself. What Sekiro is doing is aiding me in fighting against my inferiority complex by altering my mentality. Instead of determining my self worth on dying, I get happy over the minor self improvement with each death. "I'm crap at this game, I'm stupid" upon death turns to "I figured out how to deflect 2 of the bosses attacks. I can work towards deflecting the third attack, I'm making good progress." This game actually feels like a form of therapy to me.

**Phasmophobia** (248.4h, checklist format):

> After a few hours of playing this game I came to the decision: awesome. Graphics: forget what reality is. Gameplay: addictive like heroin. Audio: eargasm. PC requirements: ask NASA if they have a spare computer. Difficulty: significant brain usage. Bugs: never heard of. Hackers: there is none, just change the number of players in the lobby and make Pikachu appear. Support this game! It will be awesome!

The Doom review uses negation phrasing ("no swearing, no drugs, no killing of *people*") and the recommend thumb is YES. Sekiro's review is the only one of the six in this section's tail that VADER scores negative; the thumb is also YES. Phasmophobia's checklist is dense with words VADER reads positive ("awesome", "addictive", "eargasm") sitting alongside lines about ear-rape audio and NASA-grade hardware needs.

## Where players write milder text than they vote

Recognisable IPs with at least 50,000 reviews, ordered by alignment descending.

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

**Disappointment-AAA** owns the table. Shared traits across the rows: high-volume launches, "Mixed" Steam labels in every row, bug-mention rates between 14% and 26% on six of seven games. Apex Legends sits at 446 hours of average playtime. The reviewers in this cluster have hundreds of hours in and a red thumb on the page; VADER scores their text positive.

### The reviews behind the mild-text picks

Top review per game by `reviewInfluenceScore`, top 3 of the table above.

*Source: `Q30-alignment-drilldown.sql`*

| game | influence | upvotes | funny | comments | recommended? | playtime (h) | VADER score (-1 to +1) |
|---|---:|---:|---:|---:|---|---:|---:|
| Starfield | 0.815 | 9,037 | 172 | 719 | NO | 183.2 | +0.9969 |
| Borderlands 4 | 0.782 | 2,481 | 35 | 96 | NO | 148.5 | +0.9927 |
| Ark: Survival Ascended | 0.754 | 1,700 | 57 | 99 | NO | 7,584.4 | +0.8458 |

**Starfield** (183.2h at review, 9,037 upvotes):

> I rate Starfield 5.5/10 (an ok if mediocre BGS game that has its moments, but feels like a regression in quality from previous BGS titles). No brainer QOL features from previous Bethesda titles are absent, and the gameplay features they DID use in Starfield are weak/poorly thought out/shoe-horned in/rushed for the deadline. Starfield is a mile wide and an inch deep. The game world feels anaemic and unresponsive to player actions and decisions. Whats the point of a big open world if there's nothing interesting inside? It feels like I bought a tall glass of juice but the waiter drank 75% of it before watering it down and giving it to me for $90 CAD. In conclusion, this is a step down in quality from Bethesda compared to previous titles.

**Borderlands 4** (148.5h):

> This is a genuinely sad day for me as a HUGE Borderlands fan from day one. Ive played every Borderlands game over the years, on multiple platforms. Borderlands isnt just a series to me; its my gaming happy place. Borderlands 4 abandons that philosophy entirely. The new Vault Hunters feel bleghhh, like a Fortnite-inspired horror where everyone can do everything equally well. When every weapon can roll with parts from every manufacturer, well the result is Jakobs doesnt feel like Jakobs. Torgue doesnt feel like Torgue, well then we have lost all sense of joy. Ive maxed out achievements in Borderlands 2, Borderlands 3, and The Pre-Sequel. As Marcus once said: No refunds! Fair enough. Im not asking for one. Marcus take me back to Pandora.

**Ark: Survival Ascended** (7,584.4h at review, top-20 player by hours):

> Currently among the top 20 players with most hours. As such I cannot recommend this game to anyone. It is basically a cheap remaster (which feels like a cheap port) of Ark Survival Evolved, with content that could have been added to Ark Survival Evolved. So badly optimized, cards like a 3090 struggle to run the game on high settings. The roadmap has been delayed at least 4 times. We were promised island and scorched earth at launch (august 2023) and aberration (end of 2023). It's current roadmap puts aberration at september 4th 2024.

All three reviewers are deep-investment players (148h, 183h, 7,584h). All three reviews read as essay-length critique rather than rant. VADER scores the words positive in every case; the thumb is red in every case.

## Refunds concentrate in the early-access shooter cluster

Highest refund-rate games among titles with at least 10,000 reviews.

*Source: `Q20-refund-rates.sql`*

| Game | % refunds | review count | Steam label | text sentiment | vote rating | alignment | % early-access reviews | % bug mentions |
|---|---:|---:|---|---:|---:|---:|---:|---:|
| Bodycam | 7.17 | 23,948 | Mostly Positive | 78.33 | 77.07 | +1.26 | 100% | 10.16 |
| Atlas | 3.82 | 22,954 | Mixed | 65.26 | 53.85 | +11.41 | 100% | 24.14 |
| Escape from Tarkov | 3.57 | 10,627 | Mixed | 60.89 | 63.14 | -2.25 | 0% | 10.51 |
| Gray Zone Warfare | 3.42 | 46,021 | Mostly Positive | 77.92 | 76.64 | +1.27 | 100% | 22.19 |
| Battalion Legacy | 3.24 | 10,748 | Mixed | 71.55 | 64.72 | +6.83 | 37.04% | 9.88 |
| Killing Floor III | 3.05 | 10,269 | Mixed | 61.32 | 52.60 | +8.71 | 0% | 22.05 |
| Inzoi | 2.97 | 13,378 | Mostly Positive | 87.51 | 80.32 | +7.19 | 100% | 15.70 |
| Miscreated | 2.90 | 15,265 | Mixed | 71.25 | 68.70 | +2.55 | 68.48% | 14.77 |
| Just Survive | 2.58 | 37,607 | Mixed | 62.97 | 59.17 | +3.79 | 94.04% | 16.98 |
| Hatred | 2.58 | 11,204 | Mostly Positive | 62.20 | 82.87 | -20.67 | 0% | 4.86 |

**% refunds** and **% early-access reviews** speak volumes. Seven of the ten rows carry an early-access share above 35% (Bodycam, Atlas, Gray Zone Warfare, Battalion Legacy, Inzoi, Miscreated, Just Survive), and all seven carry an alignment above zero (text milder than vote). The alignment values stay modest (most under +12) because the underlying ratings live in the 60-80 band rather than at the ceiling. The direction (mild text, harsher vote) is the same direction the disappointment-AAA cluster runs in. Hatred is the standalone counter-example in this table: 0% early access, alignment -20.67, sitting alongside the angry-text cluster's direction.

## The pattern is structural

The same shape shows up at two other levels of the data:

- **Across all horror games**: 2,506 games and 11.1M reviews. Average alignment is **-9.23**. Thriller sits at -8.21, Survival at -6.26. Same direction as Phasmophobia at the per-game level, just averaged across the whole theme. *(Source: `Q07-theme-alignment.sql`)*
- **Across the most-reviewed games**: the top 10 by review count (CS:GO, Dota 2, Helldivers 2, Rainbow Six Siege, TF2, Terraria, GTA V, Rust, Garry's Mod, Elden Ring) all carry negative alignment. The values run from -3.02 (GTA V) to -12.18 (Rust). Same direction as the negative tail above, just at popularity grain: across these ten games the thumbs run kinder than the words. *(Source: `Q13-popularity.sql`)*

## Other notable extremes

Games with at least 1,000 reviews, ordered by absolute alignment. Q14 returns 10 rows; the table below shows 6 picked for name recognition (Outlast, Amnesia, Godus, RollerCoaster Tycoon World) plus the two unrecognisable but instructive #1 / #2 rows (Fear & Hunger, Ratshaker). Full ten in the CSV.

*Source: `Q14-most-divisive.sql`*

| Game | alignment | review count | Steam label | avg playtime (h) | % negative-text reviews | % bug mentions | % early-access reviews | genres / themes |
|---|---:|---:|---|---:|---:|---:|---:|---|
| Fear & Hunger | -34.24 | 11,480 | Very Positive | 17.6 | 38.82 | 5.63 | 0% | Indie, RPG, TBS / Fantasy, Horror, Survival |
| Ratshaker | -32.44 | 2,889 | Very Positive | 1.9 | 40.91 | 1.04 | 0% | Adventure, Indie / Action, Horror, Mystery |
| Godus | +30.36 | 5,457 | Mostly Negative | 31.2 | 37.68 | 9.22 | 100% | Indie, Point-and-click, RTS, Simulator, Strategy / 4X, Open world, Sandbox |
| RollerCoaster Tycoon World | +29.68 | 3,184 | Mostly Negative | 18.0 | 34.70 | 37.91 | 39.0% | Simulator, Strategy / Business, Sandbox |
| Amnesia: The Dark Descent | -27.68 | 20,723 | Very Positive | 14.7 | 31.23 | 2.22 | 0% | Adventure, Indie, Puzzle / Action, Horror, Survival |
| Outlast | -27.54 | 38,799 | Very Positive | 10.4 | 29.78 | 1.98 | 0% | Adventure, Indie / Action, Horror, Mystery, Stealth, Survival |

Outlast at -27.54 is the most-recognisable angry-text extreme. Godus at +30.36 (Peter Molyneux's flop) is the most-recognisable mild-text extreme.

## Sources

- Queries: `Q01-alignment-negative.sql`, `Q02-alignment-positive.sql`, `Q07-theme-alignment.sql`, `Q13-popularity.sql`, `Q14-most-divisive.sql`, `Q20-refund-rates.sql`, `Q29-alignment-explainer.sql`, `Q30-alignment-drilldown.sql` (battery preserved alongside this finding)
- Reproduce via: [DuckDB/](../../DuckDB/) (DuckDB harness over Gold parquet exports)
- Methodology: [scoring-model.md](../architecture/scoring-model.md) (§ *sentimentVoteAlignment*, § *Bayesian shrinkage with empirically-derived priors*, § *Review influence weights*)
- Companion findings: [what-sentimentrating-reveals.md](what-sentimentrating-reveals.md) (the leaderboard view of the text rating on its own), [where-the-gap-grows.md](where-the-gap-grows.md) (volume-bucket detail and the tier-mismatch reframe behind why the gap shows up where it does)
- Caveats:
  - Both ratings are post-shrinkage. Formula and priors live in `scoring-model.md`.
  - The recognisable-IP scoping for the ≥50,000-review tables is curatorial (drawn from a list of broadly familiar games to keep the prose readable), not statistical. The "Other notable extremes" section uses ≥1,000 reviews and no name filter. The refund table uses ≥10,000 reviews and no name filter.
  - Genres / themes pulled from `gold.vw_gameCatalogue`. The view is cartesian (one row per `gameKey` × theme × genre × platform), so any query reading it must collapse genres / themes per `gameKey` before joining; `Q01` and `Q02` use `STRING_AGG(DISTINCT ...)` for that. Some IGDB labels abbreviated for table width: "Role-playing (RPG)" → RPG, "Real Time Strategy (RTS)" → RTS, "Turn-based strategy (TBS)" → TBS, "Hack and slash/Beat 'em up" → Hack and slash, "Science fiction" → Sci-fi, "4X (explore, expand, exploit, and exterminate)" → 4X. "Unknown" entries (IGDB placeholder) stripped.
  - The drill-down review tables pull the single highest-`reviewInfluenceScore` review per game from `isVaderEligible = TRUE` rows, regardless of thumb direction. The negative-tail picks (Doom, Sekiro, Phasmophobia) all carry a YES thumb; the positive-tail picks (Starfield, Borderlands 4, Ark: Survival Ascended) all carry a NO thumb. `reviewInfluenceScore` weighs community engagement and review craft, not the thumb.
  - Negative-tail drill-down picks Doom, Sekiro, and Phasmophobia rather than the strict top-3 by alignment (which would be Doom, Doom Eternal, Sekiro) so both clusters named in the prose are represented.
  - Snippets are excerpted from `reviewCleaned`. Full reviews are in the Q30 CSV.
