# Funny

## What this doc covers

Steam has a separate "Funny" button below every review. This doc walks the dataset's funniest reviews and the games that attract them. It surfaces three distinct shapes: short gag reviews on widely owned games, games so poorly received that almost every review is a joke about how bad the game is, and reviews where the punchline is the playtime number.

It doesn't score humor (VADER reads sentiment, not jokes), and it doesn't restate the influence-score math (covered in [scoring-model.md](../architecture/scoring-model.md) and [edge-cases.md](edge-cases.md)).

The single sentence the doc makes: the Funny button captures several unrelated cultural phenomena, and the ratio of funny votes per review separates them.

## Glossary

| Term | Plain meaning |
|---|---|
| **funny votes** (`votesFunny`) | how many users clicked the "Funny" button on a review |
| **upvotes** (`votesUp`) | how many users clicked "Helpful" on a review |
| **recommended?** (`votedUp`) | the green thumb (YES) or red thumb (NO) the reviewer chose |
| **VADER** | a sentiment scorer that reads a review's words and returns a number from -1 (very negative) to +1 (very positive) |
| **text sentiment** (per-review label) | VADER's compound score in three buckets: Positive (≥ 0.05), Neutral, Negative (≤ -0.05) |
| **funny per review** | a per-game ratio: total funny votes divided by total reviews |
| **% negative-text reviews** (`pctNegativeSentiment`) | share of a game's reviews where VADER scored the text clearly negative |
| **Steam label** | Steam's bucket: Overwhelmingly Positive, Very Positive, Mostly Positive, Mixed, Mostly Negative, Overwhelmingly Negative |
| **theme** (`themeName`) | IGDB's review-culture tag on a game (Horror, Comedy, Erotic, Historical, etc.) |
| **hours at review** (`playtimeAtReview`) | total minutes played at the moment the review was posted, divided by 60 |

## The funniest single reviews

Top reviews dataset-wide by funny-vote count. Snippets are paraphrased to fit the table; full text in the CSV.

*Source: `Q10-funniest-reviews.sql`*

| Game | funny votes | upvotes | recommended? | text sentiment | snippet |
|---|---:|---:|---|---|---|
| Fallout 4 | 28,215 | 7,855 | YES | Neutral | "I've nearly finished making my character." |
| Fallout: New Vegas | 23,884 | 8,052 | YES | Positive | "Met a cute girl in the desert. My VATS said i'd had a 0% chance to hit that. 10/10, such realism" |
| Counter-Strike: Global Offensive | 22,190 | 8,459 | YES | Positive | "Things I gave to Counter-Strike: Money, Time, Love. Things Counter-Strike gave me: Arthritis, Rage, ..." |
| Cyberpunk 2077 | 19,624 | 18,698 | YES | Neutral | "They made a 18+ game. They also waited for everyone to turn 18+. How convenient." |
| PUBG: Battlegrounds | 15,916 | 7,058 | YES | Positive | "Fun and challenging for both you and your pc." |
| No Man's Sky | 15,731 | 8,808 | NO | Positive | "[spoiler]Almost all of which are not featured in this game.[/spoiler]" |

Nine of the top ten rows are recommended (the No Man's Sky entry is the one NO). The reviews are short (most under 100 characters) and lean on character-creator menus, dating-sim mishaps, language barriers, marketing timing, and broken hardware. Funny votes accumulate where the game is widely owned and the joke is brief.

## When the joke is the game

The same button, divided by review count, surfaces a different population. Filter to games with at least 5,000 total funny votes and order by funny per review:

*Source: `Q17b-funny-density.sql`*

| Game | funny per review | total funny | total reviews | % negative-text reviews | Steam label |
|---|---:|---:|---:|---:|---|
| Fast & Furious: Crossroads | 66.34 | 6,568 | 99 | 36.36 | Mixed |
| The Culling II | 46.84 | 5,480 | 117 | 48.72 | Mostly Negative |
| Day One: Garry's Incident | 38.84 | 49,175 | 1,266 | 51.42 | Mostly Negative |
| The Lord of the Rings: Gollum | 25.79 | 16,560 | 642 | 34.89 | Mixed |
| BrickForce | 18.88 | 31,837 | 1,686 | 38.14 | Mostly Negative |
| Genesis Online | 12.73 | 12,132 | 953 | 35.57 | Mostly Negative |
| Hunt Down the Freeman | 12.09 | 37,905 | 3,134 | 31.53 | Mixed |
| Gasp | 10.33 | 12,455 | 1,206 | 42.37 | Mostly Negative |
| The Forgotten Ones | 7.89 | 7,765 | 984 | 41.97 | Mixed |
| Call of Duty: Black Ops 7 | 9.55 | 20,982 | 2,197 | 38.37 | Mostly Negative |

Read the **funny per review** column. Fast & Furious: Crossroads sits at 66.34 funny votes per review, against a Q10 baseline where the average review carries fewer than one funny vote. Every game in the table holds a Mixed or Mostly Negative Steam label, and the % negative-text column ranges 31-49%. The shape reads as **gaming-disaster** (an informal cluster, not a measured column). Funny-per-review ratios run 10x to 66x above the Q10 baseline.

## Snippets from the disaster

Selected from the top three rows by funny-vote count for each game, one to two per game.

*Source: `Q23-disaster-snippets.sql`*

| Game | funny votes | recommended? | snippet |
|---|---:|---|---|
| Day One: Garry's Incident | 6,573 | NO | "My friend bought me this game. We are no longer friends." |
| Day One: Garry's Incident | 3,190 | YES | "Ha! Did I catch you off guard with this positive review? Nah, just wanted to show the developers what a thumbs up looks like." |
| The Lord of the Rings: Gollum | 2,301 | YES | "I love this game. My little mommy bought me this game and I love my little mommy, so I love this game. Edit: I just played the game. I hate my little mommy now." |
| The Lord of the Rings: Gollum | 1,527 | NO | "I overpaid" |
| Hunt Down the Freeman | 982 | YES | "STOP LOOKING AT THE POSITIVE REVIEWS NOW" |
| Hunt Down the Freeman | 927 | NO | "game SUCKS i go to BED" |
| The Culling II | 834 | NO | "Winner of 2018 'Dead game any%' speedrun" |
| The Culling II | 712 | NO | "I stubbed my toe on the way to install this game. That was the most enjoyable part of the experience." |
| Fast & Furious: Crossroads | 654 | NO | "Driving in Fortnite is more entertaining." |
| Fast & Furious: Crossroads | 602 | YES | "I was going to leave this a negative review. But you don't do that to family." |
| BrickForce | 2,262 | YES | "My Grandfather smoked his whole life. I was about 10 years old when my mother said to him, 'If you ever want to see your grandchildren graduate, you have to stop immediately.'... He gave it up immediately." |
| Gasp | 1,657 | YES | "a good way of making sure your steam uninstall feature still works" |
| The Forgotten Ones | 1,110 | NO | "If this game was a potato, it would be a bad potato." |
| The Forgotten Ones | 672 | NO | "Uninstall Simulator 2014" |

Several recurring shapes show up. *Uninstall-as-feature* (BrickForce, Gasp, Forgotten Ones). *Reverse-thumbs* writers using a YES rating to land a sharper punchline (Day One's "show the developers what a thumbs up looks like", Fast & Furious's "you don't do that to family"). *External comparison* ("Driving in Fortnite is more entertaining"). *Speedrun and category-name humor* (The Culling II's "Dead game any%"). The Day One: Garry's Incident "we are no longer friends" line accounts for about 13% of that game's entire funny-vote total by itself.

LOTR: Gollum's top entry is the one to read twice. The reviewer is in character as Gollum until the third sentence, then drops back to first-person to update their feelings about their mother.

## By theme: where the culture lives

Top funny review per IGDB theme. The themes act as review-culture pockets. Some are dominated by big-IP gag reviews (Sandbox, Stealth) that already showed up in Q10; the table below picks themes that surface different territory.

*Source: `Q25-funny-by-theme.sql`*

| Theme | Game | funny votes | recommended? | snippet |
|---|---|---:|---|---|
| Comedy | Undertale | 13,500 | YES | "this game made me love the two most hated fonts in history" |
| Comedy | Grand Theft Auto V | 11,574 | YES | "This review took me almost 2 years to write and was delayed 3 times." |
| Educational | Kerbal Space Program | 4,198 | YES | "Day 1: This game looks pretty good. ... Day 50: These spaceplanes are hard to build... Day 200: YES! MY 1:1 SCALE MODEL OF THE DEATH STAR IS DONE!!! Day 500: Havent seen the sun in week..." |
| Educational | Kerbal Space Program | 3,707 | YES | "Parachutes do not work on the moon. Many Kerbals died to bring us this information. 11/10" |
| Erotic | HuniePop | 5,813 | YES | "Pros: the puzzles. Cons: having to explain to my roommate why I'm playing this game; having to explain to my girlfriend why I'm playing this game and why I even bought it; trying to convince people I only got it for the puzzles" |
| Historical | Kingdom Come: Deliverance | 8,239 | YES | "on my first quest i was told to get some tools back from the town drunk. i broke into his house to try to steal them back, but the chest was locked. i tried to find the guy who sells lockpicks but gave up..." |
| Horror | Phasmophobia | 9,993 | YES | "I was being chased by the ghost, I hid in a closet, I had to fart, my mic picked up my fart. I died." |
| Drama | Life is Strange | 9,246 | YES | "When you water your plant and it says 'This action will have consequences', you know stuff just got real." |

Undertale's font joke (Comic Sans + Papyrus = Undertale's two main characters' fonts) only lands inside that game's community. The Kerbal "Day 1 to Day 500" arc reads as a player's schedule over two years of learning the game. HuniePop's Pros / Cons review is built around the social cost of owning the game. Phasmophobia's fart-and-die line lands in two beats: the ghost, the closet, the mic.

A note on Undertale: it surfaced as the top Comedy *and* top Drama *and* top Horror review in the underlying CSV (the same review for all three, because IGDB tags Undertale with all of them). Genre is overlapping by design; the same joke can sit inside three theme buckets at once.

## The playtime tail

Top 25 reviews by hours-at-review, with the funny column visible.

*Source: `Q24-playtime-tail-extended.sql`*

| Game | hours at review | funny votes | recommended? | snippet |
|---|---:|---:|---|---|
| Star Trek Online | 93,407.1 | 2 | YES | "Patrick Stewart: 'Make It So.' I've seen everything anyway. And I get on my bike and I ride off. On the grass. Sums up the game quite nicely. Best game ever." |
| Sid Meier's Civilization V | 90,980.2 | 0 | NO | "game will not play on computer even though i have owned and played this game for years" |
| Counter-Strike: Global Offensive | 77,419.4 | 0 | NO | "!" |
| Realm of the Mad God | 76,383.1 | 107 | YES | "Fun time" |
| Secret of the Magic Crystals | 73,836.1 | 8 | NO | "Game of the year, every year." |
| Black Desert | 69,983.9 | 2 | NO | "The first 42,200 hours were confusing, but these last 99 have been a blast once I figured out you can move with WASD! ... I am now at 70k hours, and I have run out of content. They have begun time gating everything." |
| Clicker Heroes | 67,326.9 | 8 | YES | "click click click win win win" |
| Idling to Rule the Gods | 66,666.0 | 0 | NO | "I lost interest in the game, it's just not the same after the first 60k hours" |
| Idle Champions of the Forgotten Realms | 66,644.2 | 0 | NO | "An endless stream of new characters... Now an endless stream of crashes and nerfs to purposefully designed mechanics. Fix your code." |
| Nekopara Vol. 1 | 75,696.9 | 3 | NO | "Imagine getting your account hacked and losing all your friends you made to this meme of a game..." |

Two shapes here. *Idle-game players reaching escape velocity* (Realm of the Mad God's "Fun time", Idling to Rule the Gods at 66,666 hours posting "lost interest after the first 60k hours", Clicker Heroes's "click click click win win win", Idle Champions's bug-report tirade). *Investment-fatigue protests* on grindy MMOs (Black Desert at 70,000 hours running out of content, Civ V refusing to launch, Dungeon Fighter Online with nine repetitions of "Don't play this game").

The Black Desert "first 42,200 hours were confusing, but these last 99 have been a blast" line is the dataset's purest grind joke. The Secret of the Magic Crystals "Game of the year, every year" review at 73,836 hours is closer to the unironic-fan end of the same shape. Both rows carry zero or near-zero funny-vote counts; the hours-at-review figure next to the text carries the weight.

## Sources

- Queries: `Q10-funniest-reviews.sql`, `Q17b-funny-density.sql`, `Q23-disaster-snippets.sql`, `Q24-playtime-tail-extended.sql`, `Q25-funny-by-theme.sql`, `Q12-longest-playtime.sql` (battery preserved alongside this finding)
- Reproduce via: [Labs/Lab03_duckdb_gold/](../../Labs/Lab03_duckdb_gold/) (DuckDB harness over Gold parquet exports)
- Methodology: [scoring-model.md](../architecture/scoring-model.md) (§ *Review influence weights*, on the community-signal sub-blend that includes funny votes)
- Companion findings: [edge-cases.md](edge-cases.md) (the gates and the playtime tail), [sentiment-vote-alignment.md](sentiment-vote-alignment.md) (text vs vote at game grain)
- Caveats:
  - Snippets are truncated (200 chars in Q10, 220 in Q24, 280 in Q23 / Q25). Spelling preserved as written; minor inline elisions marked with "..." for table width.
  - "Funny per review" is a per-game ratio. The ≥ 5,000 total-funny floor on Q17b is the volume gate that keeps a single highly-funny review on a tiny-N game from topping the list.
  - Q23 picks one or two snippets per game from each game's top three; the full top three is in `Q23-disaster-snippets.csv`.
  - Q25 joins `silver.steamreviews` to `gold.vw_gameCatalogue` on `gameKey`; the catalogue is cartesian (one row per gameKey × theme × genre × platform), so the same review can rank #1 inside multiple themes when a game is tagged with multiple. The Undertale row is the visible example.
  - Q24 returns 25 rows ordered by hours at review; the table above shows 10 curated rows. Full result in `Q24-playtime-tail-extended.csv`.
  - Q24's playtime tail contains a small number of duplicate rows where a player edited a review (the Black Desert and Idling entries appear twice with slightly different `playtimeForever` totals). Both versions are preserved in the CSV; the table above shows one copy per distinct review.
  - VADER labels in Q10 are per-review (`sentimentCompound` bucketed), not the game-grain `weightedSentimentRating`.
