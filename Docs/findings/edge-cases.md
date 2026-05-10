# Edge cases

## What this doc covers

The dataset contains reviews that are 8,000 characters of "Goat", reviews where the only word is an ASCII-art Peppa Pig, and accounts that logged 93,000 hours in one game. This doc walks those three shapes and shows which gate caught each one.

It doesn't restate the gate definitions (those live in [scoring-model.md](../architecture/scoring-model.md) and [overview.md](../architecture/overview.md)) and it doesn't re-run sentiment math (covered in [sentiment-vote-alignment.md](sentiment-vote-alignment.md) and [where-the-gap-grows.md](where-the-gap-grows.md)).

The single sentence the doc makes: the pipeline knows what kind of input it's looking at, and the gates are doing the work the design said they would.

## Glossary

| Term | Plain meaning |
|---|---|
| **VADER** | a sentiment scorer that reads a review's words and returns a number from -1 to +1 |
| **VADER-eligible** (`isVaderEligible`) | a per-review boolean. True only when the review passes every silver-side quality gate (long enough, real text, real words, ASCII shape sane) |
| **credible text** (`hasCredibleText`) | one of those gates. Catches reviews where the typical word is implausibly long, e.g. one 8,000-character "word" |
| **word-length ratio** (`wordLengthRatio`) | `reviewLength` divided by `wordCount`. A normal English review sits between 4 and 12. A Goat review hits 8,000 |
| **influence score** (`reviewInfluenceScore`) | a 0-1 weight per review used when rolling up game-level scores. Blends community signals (helpful / funny / comment / reaction votes), playtime, length, emotional intensity, and sentiment intensity |
| **weighted sentiment rating** (`weightedSentimentRating`) | per-game 0-100 score. Influence-weighted average of per-review sentiment directions (positive or negative, derived from each review's VADER compound score) across VADER-eligible reviews |
| **weighted vote rating** (`weightedVoteRating`) | per-game 0-100 score. Influence-weighted average of every thumb (yes / no), regardless of text quality |
| **CDF** (Change Data Feed) | the silver-to-gold incremental loader. Picks up changes from silver on a schedule. A row that just changed in silver may not have arrived in gold yet |

## The credible-text gate catches jokes

*Source: `Q04-goat-reviews.sql` (five non-Goat-Simulator rows below) + `Q21-goat-simulator.sql` (the three Goat Simulator rows).*

| game | reviewLength | wordCount | wordLengthRatio | snippet |
|---|---:|---:|---:|---|
| Goat Simulator | 8,000 | 1 | 8,000.0 | `GoatGoatGoatGoatGoat...` |
| Goat Simulator | 8,000 | 1 | 8,000.0 | `baaaaaaaaaaaaaaaaaaaa...` |
| Goat Simulator 3 | 7,774 | 1 | 7,774.0 | `BAHHHHHHHHHHHHHHHH...` |
| SuperHot | 8,000 | 1 | 8,000.0 | `SUPERHOTSUPERHOTSUPERHOT...` |
| Bloons TD Battles 2 | 8,000 | 1 | 8,000.0 | `syncingsyncingsyncing...` |
| Moonbase Alpha | 8,000 | 1 | 8,000.0 | `aeiouaeiouaeiouaeiou...` |
| Battlefield 2042 | 8,000 | 1 | 8,000.0 | `tankstankstankstanks...` |
| Fallout: New Vegas - Dead Money | 8,000 | 1 | 8,000.0 | `NightmareNightmareNightmare...` |

Read the **wordLengthRatio** column. Each of these reviews is one "word" between 7,774 and 8,000 characters long. The credible-text gate trips on that shape and flips `isVaderEligible` to false, so VADER never tries to score `BAHHHHHHHHHHHH` as positive or negative.

The snippets read as jokes about each game. SuperHot's catchphrase is the review. Bloons is a network-rage protest. Goat Simulator returns the noise the player makes in-game.

## How big a slice the gates cut

*Source: `Q03-eligibility-counts.sql`.*

| total reviews | pass all gates | fail any gate | % fail any gate | % fail length gate | % fail credible-text gate | % fail ASCII gate | % fail unique-word gate |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 70,953,644 | 69,326,879 | 1,626,765 | 2.29 | 1.71 | 2.19 | 0.04 | 0.07 |

Across 70.9M reviews, 2.29% fail at least one gate. The remaining columns split that 2.29% by which gate tripped (a single review can trip multiple, so the per-gate columns overlap and don't sum to 2.29). The two large gates are **length** (1.71%, mostly empty reviews where `reviewLength ≤ 1`) and **credible text** (2.19%, the word-length-ratio gate that catches the Goat shapes). The ASCII and unique-word gates each trip on less than 0.1% of the dataset.

A second cut on the same population, by review *shape*:

| empty (length ≤ 1) | goat shape (length > 1,000, wordCount = 1) | other ineligible |
|---:|---:|---:|
| 1,215,269 | 4,935 | 406,561 |

Empty reviews are the bulk of what the gates remove. The Goat shape is 4,935 of 70.9M, roughly one in 14,000 reviews. The 406,561 "other" bucket is everything else (emoji-only, ASCII-art, very short noise).

These reviews are not deleted. They survive into `gold.factreviews` with `isVaderEligible = false`, and their VADER columns return `NULL` rather than `0` ("Neutral"). Game-grain sentiment ignores them; game-grain vote keeps them.

## Influence and sentiment are different gates

`reviewInfluenceScore` is a per-review 0-1 weight. The top 5 reviews by influence:

*Source: `Q06-influence-score.sql` (Q06a CSV).*

| influence | game | upvotes | funny votes | comments | playtime at review (min) | VADER-eligible | snippet |
|---:|---|---:|---:|---:|---:|---|---|
| 0.955 | Tetris Ultimate | 229 | 77 | 24 | 1,548 | false | `http://store.steampowered.com/...` (URL only) |
| 0.927 | Titan Quest: Immortal Throne | 141 | 288 | 7 | 37,211 | false | `Чет не затягивает` (Russian, 3 words) |
| 0.926 | My Friend Peppa Pig | **2,507** | 921 | 56 | 309 | false | (ASCII art, full snippet below) |
| 0.904 | The Sims 4: Nifty Knitting Stuff | 165 | 234 | 10 | 0 | true | `This addon pack for Sims 4 is all about adding METAL to music choices...` |
| 0.892 | Big Money! | 128 | 61 | 9 | 60,580 | true | `This. Freakin. Game. This game is a masterpiece, easily the best game of 2002...` |

The reading column is **VADER-eligible**. Three of the top five are false. The Peppa Pig review has 2,507 upvotes, 921 funny votes, 56 comments, and 309 minutes of playtime. Influence is 0.926. `isVaderEligible` is false, so that weight applies to the per-game vote rollup only, not the per-game sentiment rollup.

The Peppa Pig snippet in full:

```
⣿⣿⣿⣿⣿⣿⣿⣿⡿⠿⠛⠛⠛⠋⠉⠈⠉⠉⠉⠉⠛⠻⢿⣿⣿⣿⣿⣿⣿⣿
⣿⣿⣿⣿⣿⡿⠋⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠉⠛⢿⣿⣿⣿⣿
⣿⣿⣿⣿⡏⣀⠀⠀⠀⠀⠀⠀⠀⣀⣤⣤⣤⣄⡀⠀⠀⠀⠀⠀⠀⠀⠙⢿⣿⣿
⣿⣿⣿⢏⣴⣿⣷⠀⠀⠀⠀⠀⢾⣿⣿⣿⣿⣿⣿⡆⠀⠀⠀⠀⠀⠀⠀⠈⣿⣿
⣿⣿⣟⣾⣿⡟⠁⠀⠀⠀⠀⠀⢀⣾⣿⣿⣿⣿⣿⣷⢢⠀⠀⠀⠀⠀⠀⠀⢸⣿
⣿⣿⣿⣿⣟⠀⡴⠄⠀⠀⠀⠀⠀⠀⠙⠻⣿⣿⣿⣿⣷⣄⠀⠀⠀⠀⠀⠀⠀⣿
⣿⣿⣿⠟⠻⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠶⢴⣿⣿⣿⣿⣿⣧⠀⠀⠀⠀⠀⠀⣿
⣿⣁⡀⠀⠀⢰⢠⣦⠀⠀⠀⠀⠀⠀⠀⠀⢀⣼⣿⣿⣿⣿⣿⡄⠀⣴⣶⣿⡄⣿
⣿⡋⠀⠀⠀⠎⢸⣿⡆⠀⠀⠀⠀⠀⠀⣴⣿⣿⣿⣿⣿⣿⣿⠗⢘⣿⣟⠛⠿⣼
⣿⣿⠋⢀⡌⢰⣿⡿⢿⡀⠀⠀⠀⠀⠀⠙⠿⣿⣿⣿⣿⣿⡇⠀⢸⣿⣿⣧⢀⣼
⣿⣿⣷⢻⠄⠘⠛⠋⠛⠃⠀⠀⠀⠀⠀⢿⣧⠈⠉⠙⠛⠋⠀⠀⠀⣿⣿⣿⣿⣿
⣿⣿⣧⠀⠈⢸⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠟⠀⠀⠀⠀⢀⢃⠀⠀⢸⣿⣿⣿⣿
⣿⣿⡿⠀⠴⢗⣠⣤⣴⡶⠶⠖⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣀⡸⠀⣿⣿⣿⣿
⣿⣿⣿⡀⢠⣾⣿⠏⠀⠠⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠛⠉⠀⣿⣿⣿⣿
⣿⣿⣿⣧⠈⢹⡇⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣰⣿⣿⣿⣿
⣿⣿⣿⣿⡄⠈⠃⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣠⣴⣾⣿⣿⣿⣿⣿
⣿⣿⣿⣿⣧⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣠⣾⣿⣿⣿⣿⣿⣿⣿⣿⣿
⣿⣿⣿⣿⣷⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣴⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿
⣿⣿⣿⣿⣿⣦⣄⣀⣀⣀⣀⠀⠀⠀⠀⠘⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿
⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣷⡄⠀⠀⠀⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿
⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣧⠀⠀⠀⠙⣿⣿⡟⢻⣿⣿⣿⣿⣿⣿⣿⣿⣿
⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⠇⠀⠁⠀⠀⠹⣿⠃⠀⣿⣿⣿⣿⣿⣿⣿⣿⣿
⣿⣿⣿⣿⣿⣿⣿⣿⡿⠛⣿⣿⠀⠀⠀⠀⠀⠀⠀⠀⢐⣿⣿⣿⣿⣿⣿⣿⣿⣿
⣿⣿⣿⣿⠿⠛⠉⠉⠁⠀⢻⣿⡇⠀⠀⠀⠀⠀⠀⢀⠈⣿⣿⡿⠉⠛⠛⠛⠉⠉
⣿⡿⠋⠁⠀⠀⢀⣀⣠⡴⣸⣿⣇⡄⠀⠀⠀⠀⢀⡿⠄⠙⠛⠀⣀⣠⣤⣤⠄
```

I am just as confused.

Influence answers "how much should this review's vote count for the game total." Sentiment eligibility answers "does the text deserve to be scored at all." The two gates run independently.

## The playtime tail

*Source: `Q12-longest-playtime.sql`.*

| game | hours at review | upvotes | recommended? | snippet |
|---|---:|---:|---|---|
| Star Trek Online | 93,407.1 | 3 | YES | `Patrick Stewart: "Make It So." I've seen everything anyway. And I get on my bike and I ride off...` |
| Sid Meier's Civilization V | 90,980.2 | 2 | NO | `game will not play on computer even though i have owned and played this game for years...` |
| Sid Meier's Civilization IV | 81,336.3 | 0 | YES | `Pretty good.` |
| Idling to Rule the Gods | 81,015.5 | 0 | YES | `best idle game around.` |
| Team Fortress 2 | 79,609.9 | 1,451 | YES | `As the player who has logged more hours in TF2 than anyone else, I highly reccomend this game.` |

93,407.1 hours is about 10.6 years of continuous play. The pipeline does not flag or drop these rows; the values come straight from Steam and feed into the playtime component of the influence score. `playtimeAtReview` ranges this wide, and the values are preserved in `silver.steamreviews` for any future bot-detection filter to act on.

Idling to Rule the Gods at 81,015.5 hours fits the game's mechanic: it's an incremental clicker designed to run in the background.

## Sources

- Queries: `Q03-eligibility-counts.sql`, `Q04-goat-reviews.sql`, `Q12-longest-playtime.sql`, `Q21-goat-simulator.sql` (battery preserved alongside this finding); influence drill-down: `Q06a-influence-top.csv` (CSV only, no backing SQL preserved)
- Reproduce via: [Labs/Lab03_duckdb_gold/](../../Labs/Lab03_duckdb_gold/) (DuckDB harness over Gold parquet exports)
- Methodology:
  - [scoring-model.md](../architecture/scoring-model.md) (§ *Review eligibility gates*, § *Review influence weights*)
  - [overview.md](../architecture/overview.md) (silver-to-gold CDF loader)
- Companion findings: [sentiment-vote-alignment.md](sentiment-vote-alignment.md), [where-the-gap-grows.md](where-the-gap-grows.md), [protest-reviews.md](protest-reviews.md)
- Caveats:
  - The Q04 + Q21 row shapes shown above are subsets of each query's full output. `asciiRatio`, `uniqueWordRatio`, and `hasCredibleText` are present in the underlying CSVs but trimmed from the table for readability.
  - The Q06a "VADER-eligible" column is the `isVaderEligible` flag. The five-signal influence-score blend (community / playtime / length / emotional / sentiment) is documented in [scoring-model.md](../architecture/scoring-model.md) § *Review influence weights*.
  - The 93,407.1-hour Star Trek Online figure is `silver.steamreviews.playtimeAtReview` (minutes) divided by 60 and rounded to one decimal, as Q12 reports it.
  - "Recommended?" in the Q12 table maps to `votedUp` (YES = green thumb, NO = red thumb).
