# Protest reviews

## What this doc covers

Steam's "most helpful" tab on a game's review page sorts by community upvotes. In this dataset the top of that leaderboard is not quality reviews. It is organised backlash: about a publisher decision, a launch's quality, a terms-of-service change. This doc walks the top ten, points out that one game (Helldivers 2) lands four of the ten entries, and shows that two queries written for different reasons converge on the same Helldivers 2 PSN-account protest review.

The pipeline scores reviews with an influence-weighted blend documented in [scoring-model.md](../architecture/scoring-model.md). The per-game normalisation that makes the influence score work across games of wildly different sizes is the subject of [ADR-007](../adrs/adr-007-per-game-normalisation.md), which is also the reason one of the queries below exists in the first place.

## Glossary

The tables below use these terms. The bracketed name is the column name in the underlying data.

| Term | Plain meaning |
|---|---|
| **upvotes** (`votesUp`) | how many Steam users marked the review "helpful" |
| **funny votes** (`votesFunny`) | the separate "Funny" button below each review |
| **comments** (`commentCount`) | comment-thread posts on the review (zero when the author disabled the thread) |
| **recommended?** (`votedUp`) | the green thumb (YES) or red thumb (NO) the reviewer chose |
| **hours at review** | the player's total playtime when they posted the review |
| **VADER** | a sentiment scorer that reads a review's words and returns a number from -1 to +1 |
| **text sentiment** (per-review label) | the review's VADER compound score in three buckets: Positive (≥ 0.05), Neutral, Negative (≤ -0.05) |
| **text sentiment rating** (per-game, 0-100) | every review's sentiment direction (positive or negative, derived from the review's VADER compound score), weighted by per-review influence score, averaged per game, then smoothed; column name `weightedSentimentRating` |
| **weighted vote rating** (per-game, 0-100) | every thumb (YES = +1, NO = -1), weighted by influence score, averaged per game, then smoothed; column name `weightedVoteRating` |
| **influence score** | a 0-1 weight per review that blends community signals (helpfulness / funny / comment / reaction votes), playtime, review length, the strength of the writer's emotional intensity, and sentiment intensity |
| **smoothing** | a step that nudges low-volume games toward the dataset average so a 5-review indie doesn't outrank a 50,000-review behemoth (technical name: empirical-Bayes shrinkage) |
| **Steam label** | Steam's own bucket: Overwhelmingly Positive, Very Positive, Mostly Positive, Mixed, Mostly Negative, Overwhelmingly Negative |

## The leaderboard

*Source: `Q11a-most-helpful-reviews.sql`*

| Game | upvotes | funny votes | comments | recommended? | hours at review | text sentiment | what the review is about |
|---|---:|---:|---:|---|---:|---|---|
| Dragon's Dogma II | 38,484 | 12,080 | 884 | NO | 4.1 | Positive | "good review DLC for $1.99", microtransactions protest |
| Battlefield 2042 | 36,261 | 570 | 900 | NO | 32.6 | Negative | launch-quality complaint |
| Borderlands 2 | 36,112 | 432 | 0 | NO | 343.7 | Positive | Take-Two ToS update, spyware concern |
| Helldivers 2 | 34,139 | 503 | 1,821 | NO | 2.1 | Negative | required PSN account |
| Rust | 28,267 | 327 | 775 | YES | 114.4 | Positive | in-game story about taking a player prisoner |
| Helldivers 2 | 27,147 | 823 | 393 | YES | 44.2 | Positive | "DEMOCRACY HAS PREVAILED" (after the PSN reversal) |
| Dragon's Dogma II | 27,039 | 1,153 | 0 | NO | 0.2 | Positive | DRM banned the player's Linux/Proton test rigs |
| Helldivers 2 | 22,792 | 3,826 | 191 | YES | 14.0 | Positive | "FOR THE LOVE OF LIBERTY" |
| Helldivers 2 | 22,370 | 750 | 0 | NO | 33.5 | Positive | nProtect Gameguard kernel-level access |
| Dragon's Dogma II | 22,202 | 261 | 0 | NO | 0.5 | Negative | Denuvo and microtransactions |

The headline column is **recommended?**: seven rows out of ten read NO. The most-helpful slot, in this dataset, is dominated by reviews telling other players not to buy.

Three games supply nine of the ten rows. **Helldivers 2 lands four** entries. **Dragon's Dogma II lands three**. Battlefield 2042, Borderlands 2, and Rust contribute one each.

**Hours at review** tells two stories. The Dragon's Dogma II top entry was posted at 4.1 hours played; the Helldivers 2 PSN protest at 2.1 hours. These are not deep-investment reviews, they are early ones used as a rallying point. The Borderlands 2 entry is the opposite: 343.7 hours, written years into ownership and triggered by a publisher policy change.

**Text sentiment** is also worth pausing on. Most NO entries read as Positive. Protest writing is usually politely worded ("please reconsider", "do not buy at this time") rather than angry, and VADER scores the words, not the position the review takes.

The **comments** column has four zeros. Those reviewers disabled the thread after the review went viral. The Dragon's Dogma II top entry's text says it directly: *"Discussion has run its course so I've disabled comments."*

## The Helldivers 2 arc

Helldivers 2 is the only game whose multiple entries read together as a sequence. Pulling its four rows out of the leaderboard above:

| upvotes | funny votes | recommended? | what the review is about |
|---:|---:|---|---|
| 34,139 | 503 | NO | required PSN account (before the reversal) |
| 27,147 | 823 | YES | "DEMOCRACY HAS PREVAILED" (after the reversal) |
| 22,792 | 3,826 | YES | "FOR THE LOVE OF LIBERTY" |
| 22,370 | 750 | NO | nProtect Gameguard kernel-level access (separate issue) |

Two NO entries flag publisher and anti-cheat decisions. Two YES entries celebrate Sony rolling back the PSN requirement. The same community used the most-helpful slot first to apply pressure, then to mark the climbdown.

The third row carries **3,826 funny votes**, by far the highest funny count among the four. The joke ("FOR THE LOVE OF LIBERTY") landed as a meme alongside the protest.

## Cross-validation: two queries, one review

The Helldivers 2 PSN protest also shows up in a query written for an entirely different reason. `Q05-pergame-norm.sql` was authored to demonstrate why the per-review influence score has to normalise upvotes **per game** rather than across the whole dataset (see [ADR-007](../adrs/adr-007-per-game-normalisation.md): a global normalisation ceiling would let one Counter-Strike review dwarf every indie's most-upvoted review). The query asks: among the five highest-volume games on Steam, what is the most-upvoted single review on each?

*Source: `Q05-pergame-norm.sql`*

| game | total reviews | max single upvotes | also in Q11a top 10? |
|---|---:|---:|---|
| Counter-Strike: Global Offensive | 2,514,949 | 20,454 | no |
| Dota 2 | 823,817 | 13,971 | no |
| Helldivers 2 | 794,791 | 34,139 | yes (position 4, the PSN protest) |
| Rainbow Six Siege | 764,759 | 7,989 | no |
| Team Fortress 2 | 728,364 | 8,949 | no |

Helldivers 2 is the only game in this list whose top single review also lands in the dataset-wide top ten. Counter-Strike has more than three times the review count, yet its most-upvoted review (20,454) sits well below Helldivers 2's PSN protest (34,139). [ADR-007](../adrs/adr-007-per-game-normalisation.md) covers that gap. Without per-game normalisation a 34,139-upvote outlier on Helldivers 2 would push the influence-score ceiling for the whole dataset, and a popular indie review with a few hundred upvotes would round to near-zero. With per-game normalisation the same review acts as a top community signal within Helldivers 2's own review population, without skewing the indie game next door.

(Three games higher up the dataset-wide leaderboard, Dragon's Dogma II, Battlefield 2042, and Borderlands 2, have single reviews with even more upvotes than the PSN protest. None of those three are top-five-volume games and none land four entries in Q11a.)

## One protest review can shape a small corpus

Stepping out from single reviews to game totals: which games sit highest by aggregate upvotes, and what does the per-review average look like?

*Source: `Q11b-aggregate-upvotes.sql`*

| game | total upvotes | total reviews | upvotes per review | text sentiment rating | weighted vote rating | Steam label |
|---|---:|---:|---:|---:|---:|---|
| Counter-Strike: Global Offensive | 1,905,582 | 2,514,949 | 0.76 | 76.41 | 84.82 | Very Positive |
| Helldivers 2 | 983,412 | 794,791 | 1.24 | 77.32 | 81.69 | Very Positive |
| Battlefield 2042 | 709,638 | 132,589 | 5.35 | 57.32 | 46.93 | Mixed |

The reading column is **upvotes per review**. Counter-Strike at 0.76 is roughly the dataset baseline: the average Counter-Strike review gets fewer than one upvote. Helldivers 2 at 1.24 is elevated. Battlefield 2042 at 5.35 is the outlier: the average review on Battlefield 2042 carries more than five upvotes.

That number is not every reviewer upvoting every other reviewer. Battlefield 2042 has 132,589 reviews, and one of them is the launch-quality complaint that sits at row 2 of the leaderboard with 36,261 upvotes by itself. That single review accounts for roughly **5% of the entire Battlefield 2042 community-vote budget**. The game's Steam label is **Mixed** and its weighted vote rating sits at **46.93**. Both are pulled down by the same wave of protest the review represents, though the per-game normalisation (per ADR-007 again) keeps that one row from dominating the per-game aggregate outright.

On a corpus this size, one review can shift the per-game numbers it sits in.

## Sources

- Queries: `Q05-pergame-norm.sql`, `Q11a-most-helpful-reviews.sql`, `Q11b-aggregate-upvotes.sql` (battery preserved alongside this finding)
- Reproduce via: [DuckDB/](../../DuckDB/) (DuckDB harness over Gold parquet exports)
- Methodology:
  - [scoring-model.md](../architecture/scoring-model.md) (§ *Review influence weights*)
  - [ADR-007, per-game normalisation](../adrs/adr-007-per-game-normalisation.md) (why Q05 was written; why a single 34k-upvote review does not unilaterally dominate per-game aggregates)
- Companion: [sentiment-vote-alignment.md](sentiment-vote-alignment.md) (text vs vote at game grain; protest reviews illustrate why the two axes diverge at the single-review level)
- Caveats:
  - "What the review is about" is hand-summarised from the 200-character snippet returned by Q11a (full snippet column in the CSV).
  - Q05's high-volume bucket surfaces the loudest single review within the top-five-volume games as a side effect; its primary purpose is to motivate per-game normalisation.
  - Text-sentiment labels are VADER on the review's words, not on the act of protesting. A politely worded "do not buy" reads as Positive to the scorer.
