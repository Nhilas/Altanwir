# What sentiment rating reveals

## What this doc covers

The pipeline's per-game text-sentiment number is `weightedSentimentRating`, a 0-100 score. It runs VADER (a sentiment scorer that reads each review's text and returns a number from -1 to +1) over every review that passes silver-side quality gates, derives a positive-or-negative direction from each score, weights those directions by a per-review *influence score*, averages per game, and pulls low-volume titles toward the dataset average so a 5-review indie does not outrank a 50,000-review behemoth. This doc reads what tops that leaderboard at game grain, then asks whether the same shape shows up at theme and genre grain.

It doesn't dwell on the gap between text and votes (covered in [sentiment-vote-alignment.md](sentiment-vote-alignment.md)) and it doesn't restate the smoothing formulas or priors (those live in [scoring-model.md](../architecture/scoring-model.md)).

The single sentence the doc makes: after the math, the smoothed top reads as cozy, story-led, audience-tested games, and that same shape repeats one level up at theme and genre grain.

## Glossary

| Term | Plain meaning |
|---|---|
| **VADER** | a sentiment scorer that reads a review's words and returns a number from -1 to +1 |
| **compound score** | VADER's output number for a single review, ranging -1 to +1. Above +0.05 reads as positive; below -0.05 reads as negative; in between reads as neutral. |
| **text sentiment rating** (`weightedSentimentRating`) | per-game 0-100 score. Influence-weighted average of per-review sentiment directions (positive or negative, derived from each review's VADER compound score) across VADER-eligible reviews, then smoothed. |
| **vote rating** (`weightedVoteRating`) | per-game 0-100 score. Influence-weighted average of every thumb (yes / no), then smoothed. |
| **influence score** (`reviewInfluenceScore`) | a 0-1 weight per review. Blends five signals: community votes (helpful / funny / comment / reaction), playtime, review length, emotional intensity, sentiment intensity. |
| **smoothing** (empirical-Bayes shrinkage) | a step that nudges low-volume games toward the dataset average so a 5-review indie does not outrank a 50k-review behemoth. Formula and priors in `scoring-model.md`. |
| **% negative-text reviews** (`pctNegativeSentiment`) | share of a game's reviews where VADER scored the text clearly negative (compound score ≤ -0.05) |
| **% refunds** (`pctRefunded`) | share of a game's reviews written by accounts that refunded the game |
| **% bug mentions** (`pctBugReports`) | share of reviews mentioning *bug, crash, error, lag, stuck,* or *glitch* |
| **Steam label** (`steamRatingLabel`) | Steam's own bucket: Overwhelmingly Positive, Very Positive, Mostly Positive, Mixed, Mostly Negative, Overwhelmingly Negative. Buckets depend on review count (see `semantic-layer-lite.md` §4). |
| **rated games** (`ratedGames`) | how many distinct games sit under a theme or genre with at least one rating |

## Contents

- [The smoothed top of the leaderboard](#the-smoothed-top-of-the-leaderboard)
  - [The reviews behind the top 3](#the-reviews-behind-the-top-3)
- [Theme aggregates trend the same way](#theme-aggregates-trend-the-same-way)
  - [Business, Educational, Romance: theme drill-down](#business-educational-romance-theme-drill-down)
- [Genre aggregates also follow the shape](#genre-aggregates-also-follow-the-shape)
  - [Point-and-click, Puzzle, Platform: genre drill-down](#point-and-click-puzzle-platform-genre-drill-down)
- [The golden zone: where genre and theme meet](#the-golden-zone-where-genre-and-theme-meet)
- [The pattern is structural](#the-pattern-is-structural)
- [Sources](#sources)

## The smoothed top of the leaderboard

Top 15 games by text sentiment rating, scoped to titles with at least 10,000 reviews.

*Source: `Q23-top-sentimentrated-games.sql`*

| Game | text sentiment | vote rating | reviews | Steam label | avg playtime (h) | % negative-text reviews | % refunds | % bug mentions | genres / themes |
|---|---:|---:|---:|---|---:|---:|---:|---:|---|
| A Short Hike | 96.75 | 98.55 | 14,349 | Overwhelmingly Positive | 4.4 | 2.62 | 0.16 | 0.86 | Adventure, Indie / Fantasy, Open world |
| Fields of Mistria | 96.47 | 97.71 | 21,842 | Overwhelmingly Positive | 37.5 | 3.16 | 0.24 | 7.05 | Indie, RPG, Simulator / Open world, Romance, Sandbox |
| Tiny Glade | 96.21 | 97.19 | 14,206 | Very Positive | 9.4 | 3.25 | 0.76 | 1.40 | Indie, Simulator / Sandbox |
| Chants of Sennaar | 95.86 | 97.71 | 12,935 | Overwhelmingly Positive | 11.4 | 3.74 | 0.05 | 3.49 | Adventure, Indie, Puzzle / Fantasy, Mystery, Stealth |
| Tactical Breach Wizards | 95.49 | 97.71 | 10,539 | Overwhelmingly Positive | 17.0 | 4.29 | 0.22 | 2.36 | Adventure, Indie, Strategy, Tactical, TBS / Action, Comedy, Fantasy, Sci-fi |
| Sheepy: A Short Adventure | 95.44 | 98.17 | 10,917 | Overwhelmingly Positive | 2.2 | 4.11 | 0.00 | 2.08 | Adventure, Indie, Platform / Fantasy, Horror, Mystery, Survival |
| The Room | 95.15 | 97.08 | 14,761 | Very Positive | 4.6 | 4.44 | 0.14 | 5.91 | Indie, Point-and-click, Puzzle / Fantasy |
| The Room Two | 95.05 | 97.14 | 10,396 | Very Positive | 4.9 | 4.68 | 0.07 | 4.83 | Indie, Point-and-click, Puzzle / Fantasy, Mystery |
| Islanders | 94.97 | 95.70 | 10,550 | Very Positive | 12.1 | 4.90 | 0.75 | 1.54 | Indie, Puzzle, Strategy, TBS / Sandbox |
| Unpacking | 94.96 | 93.84 | 25,585 | Very Positive | 7.9 | 4.83 | 0.25 | 0.89 | Indie, Point-and-click, Puzzle, Simulator / Drama |
| Dorfromantik | 94.90 | 96.32 | 16,539 | Very Positive | 24.7 | 4.70 | 0.32 | 1.10 | Card & Board Game, Indie, Puzzle, Simulator, Strategy, TBS / Non-fiction, Sandbox |
| Abzu | 94.86 | 93.75 | 12,625 | Very Positive | 4.0 | 5.00 | 0.70 | 1.77 | Adventure, Indie, Puzzle / Action, Educational, Fantasy |
| Our Life: Beginnings & Always | 94.70 | 98.41 | 11,259 | Overwhelmingly Positive | 42.5 | 4.98 | 0.00 | 0.83 | Indie, Simulator, Visual Novel / Comedy, Romance |
| Townscaper | 94.61 | 95.92 | 11,207 | Very Positive | 8.0 | 4.81 | 0.32 | 1.40 | Indie, Simulator / Sandbox |
| Strange Horticulture | 94.59 | 95.36 | 10,168 | Very Positive | 8.5 | 5.19 | 0.12 | 2.76 | Adventure, Puzzle, RPG, Simulator / Mystery |

The first column to read is **text sentiment**: every row sits between 94.59 and 96.75. The vote rating column tracks within a few points of the text rating across the table; sentiment-vote alignment stays small here (the inverse picture from the negative tail in the alignment doc).

Two informal clusters jump out (a read of the games, not separate measured columns). The first reads as **cozy / sandbox**: Tiny Glade, Townscaper, Dorfromantik, Islanders, Fields of Mistria, Unpacking. Open-ended building or arrangement, no fail state. Avg playtime sits between 8h and 38h. The second reads as **short narrative-puzzle**: A Short Hike, Chants of Sennaar, Sheepy, The Room, The Room Two, Abzu, Strange Horticulture, Tactical Breach Wizards. Tight runtimes (2-17h on average), story or puzzle as the load-bearing draw.

The behavior columns line up. **% refunds** sits at 0.76 or below across all 15 rows; **% bug mentions** sits at 7.05 or below (and at 3.49 or below for 11 of 15). **% negative-text reviews** ranges from 2.62 to 5.19, well below the dataset's average pctNegativeSentiment band (the same column hits 16-25 across the alignment doc's tail).

## The reviews behind the top 3

The three games at the top of the 10,000-review leaderboard each produced one review ranked first by `reviewInfluenceScore`. That score blends five signals: community votes (weight 1.5), playtime (1.0), sentiment intensity (1.0), review length (0.5), and emotional intensity (0.3). The table shows the raw inputs to those signals.

*Source: `Q25-game-review-drilldown.sql`*

| game | influence | upvotes | funny votes | comments | playtime (h) | VADER score (-1 to +1) |
|---|---:|---:|---:|---:|---:|---:|
| A Short Hike | 0.830 | 1,033 | 13 | 326 | 7.0 | 0.997 |
| Fields of Mistria | 0.720 | 283 | 3 | 3 | 150.9 | 0.976 |
| Tiny Glade | 0.810 | 2,372 | 1,086 | 14 | 11.3 | 0.864 |

Three paths to a high influence score. A Short Hike's review drew 326 comments; Tiny Glade's drew 2,372 upvotes and 1,086 funny votes; Fields of Mistria's was written at 150.9 hours of play.

**A Short Hike** (walkthrough format, VADER 0.997):

> You are a young Bird Girl named Claire who is visiting her Aunt on an Island. First your skills are very bad. You can learn to glide. You get your first Quest to get 15 Seashells. You have to explore and speak with People and help them. You find a Pickaxe to open Mines for Shortcuts. Main Reason is to get on the Top of the Island. To get there you need at least 7 of 20 Gold Feathers. There are 3 Races to beat too. Nice Island Design.

**Fields of Mistria** (150.9h at review, VADER 0.976):

> Friends ask me how I could like a "Stardew knockoff" considering I've logged over 1,000+ hours into that game. I tell them its because Mistria is not Stardew. It is its own farming sim. One that refreshes the genre 10 years after Stardew launched. The low risk high reward format of this game keeps it so cozy and sucks the player in for hours. We aren't pigeon holed into the need for doing EVERYTHING, we can farm if we want, raise animals if we want. The game progresses regardless from you doing what YOU want to do.

**Tiny Glade** (2,372 upvotes, 1,086 funny votes, VADER 0.864):

> I only wish there were chickens in this game. What's the point of putting up fencing if there aren't any chickens? What kind of glade doesn't have chickens? I feel like I'm missing out on ambient clucking as well. Looking forward to DLC (DownLoadable Chickens) to fill the egg-shaped hole in my heart.

## Theme aggregates trend the same way

Top 10 themes by text sentiment rating, scoped to themes covering at least 100 rated games.

*Source: `Q24-aggregate-sentiment-leaders.sql` (theme CSV)*

| theme | rated games | reviews | text sentiment | vote rating | % negative-text reviews | avg playtime (h) | % refunds | % bug mentions |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Business | 411 | 1,646,889 | 88.65 | 91.11 | 9.90 | 43.3 | 0.33 | 5.47 |
| Educational | 496 | 606,354 | 87.44 | 91.12 | 10.91 | 33.2 | 0.37 | 4.74 |
| Romance | 452 | 930,386 | 86.97 | 93.45 | 11.92 | 17.6 | 0.18 | 2.74 |
| Kids | 1,119 | 1,574,244 | 86.77 | 89.92 | 11.06 | 14.0 | 0.37 | 4.28 |
| Mystery | 1,258 | 2,755,412 | 85.39 | 90.68 | 13.85 | 10.9 | 0.33 | 4.78 |
| Drama | 777 | 3,122,454 | 84.39 | 89.60 | 14.16 | 20.4 | 0.28 | 4.63 |
| Comedy | 2,908 | 7,551,293 | 84.18 | 90.27 | 13.48 | 15.3 | 0.29 | 3.54 |
| 4X | 210 | 1,033,994 | 84.16 | 85.15 | 14.57 | 86.4 | 0.29 | 7.01 |
| Party | 910 | 1,515,974 | 84.12 | 88.93 | 13.00 | 15.6 | 0.41 | 3.39 |
| Erotic | 278 | 216,046 | 83.86 | 88.63 | 14.89 | 19.1 | 0.54 | 2.71 |

Read the **theme** column. Business / Educational / Romance / Kids / Mystery / Drama / Comedy land seven of the top ten. Themes like Horror, Thriller, and Survival do not appear in this top ten. Their text sentiment ratings sit a few points below the lowest row above; the alignment doc covers how their text and vote ratings diverge. **% negative-text reviews** at theme grain stays under 15 in every row above; **% bug mentions** stays under 8.

## Business, Educational, Romance: theme drill-down

Top 3 games per theme. `allThemes` reflects every theme tag the game carries. The influence, upvotes, funny, and playtime columns describe the single highest-ranked review per game.

*Source: `Q26-theme-game-drilldown.sql`*

| theme | game | text sentiment | reviews | allThemes | influence | upvotes | funny | playtime (h) |
|---|---|---:|---:|---|---:|---:|---:|---:|
| Business | Tiny Bookshop | 96.13 | 4,690 | Business | 0.864 | 308 | 120 | 58.6 |
| Business | Sticky Business | 95.53 | 5,459 | Business | 0.711 | 141 | 42 | 14.6 |
| Business | Megaquarium | 94.73 | 2,562 | Business, Sandbox | 0.703 | 317 | 14 | 42.2 |
| Educational | Alba: A Wildlife Adventure | 96.34 | 3,838 | Educational, Kids, Open world | 0.725 | 144 | 2 | 5.1 |
| Educational | Please, Touch The Artwork 2 | 95.97 | 2,230 | Educational, Historical | 0.724 | 29 | 0 | 2.8 |
| Educational | Abzu | 94.86 | 12,625 | Action, Educational, Fantasy | 0.755 | 621 | 14 | 5.4 |
| Romance | Fields of Mistria | 96.47 | 21,842 | Open world, Romance, Sandbox | 0.720 | 283 | 3 | 150.9 |
| Romance | Wylde Flowers | 95.92 | 1,969 | Fantasy, Mystery, Romance | 0.781 | 60 | 5 | 109.4 |
| Romance | Roots of Pacha | 95.88 | 2,874 | Romance, Sandbox | 0.837 | 168 | 165 | 133.5 |

**Tiny Bookshop** (58.6h at review):

> I have never in my life finished a game and immediately had to go back and finish the storylines I had left, until I played this game.

**Sticky Business** (14.6h):

> This game has cured my depression, given me 20/20 vision, cleared my skin, filed my taxes, and saved my soul from eternal damnation. Anxiety, WHO? Mental instability? Never heard of her.

**Megaquarium** (42.2h):

> I have autism, and pretty bad depression, and this game helps bring me out of depressive days and sour moods. I can stare down at my fish tanks and watch them swim around, watch the guests come and go as they enjoy what I've created. It just brings a smile to my face.

**Alba: A Wildlife Adventure** (5.1h, reviewer age 8):

> My name is Helena and Im 8 years old and this is my review. I like it very much because it is educational in right way, graphics is nice especially from the castle. I like it because it offers me a lot of choice. It is simply fun.

**Please, Touch The Artwork 2** (2.8h, full review):

> Extremely relaxing and creative point and click. We control a skeleton character in different environments, collecting objects hidden in beautiful works of art, and thus completing objectives. Excellent free game, fantastic ambient music and beautiful art direction.

**Abzu** (5.4h):

> This is not really a game, it's an experience. The game is a dream come true for someone like me who loves to just enjoy the surroundings and take lots of screens. There are PLENTY of opportunities.

**Fields of Mistria** review in the game drill-down above.

**Wylde Flowers** (109.4h):

> Life sim, farming, magic and gay marriage, Hell yeah! This game is the perfect mixture of everything I love. Magic, farming, quests, mysteries, relationships, gays and quirky graphics. The game is really one of a kind, its kind of like playing a game and watching a movie at the same time.

**Roots of Pacha** (133.5h, 165 funny votes):

> I managed to marry a guy by only giving him poop! And he loved! I didn't know I could marry my bf/coop partner, so I married the poop guy. Then my bf thought it was a good idea to marry the poop guy sister. It was probably revenge.

## Genre aggregates also follow the shape

Top 10 genres, same scope.

*Source: `Q24-aggregate-sentiment-leaders.sql` (genre CSV)*

| genre | rated games | reviews | text sentiment | vote rating | % negative-text reviews | avg playtime (h) | % refunds | % bug mentions |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| Point-and-click | 1,277 | 1,351,194 | 87.81 | 91.81 | 11.17 | 11.2 | 0.24 | 4.23 |
| Puzzle | 4,730 | 6,776,415 | 86.25 | 92.15 | 12.82 | 11.6 | 0.30 | 4.65 |
| Platform | 4,099 | 6,224,777 | 86.00 | 92.13 | 12.30 | 10.8 | 0.26 | 3.88 |
| Visual Novel | 808 | 1,206,460 | 85.41 | 93.23 | 13.35 | 19.3 | 0.22 | 2.11 |
| Card & Board Game | 595 | 1,254,088 | 85.35 | 86.26 | 13.17 | 34.6 | 0.29 | 3.82 |
| Music | 672 | 938,134 | 85.31 | 92.90 | 12.14 | 14.4 | 0.23 | 2.12 |
| Turn-based strategy (TBS) | 1,215 | 2,990,982 | 84.09 | 87.81 | 14.80 | 49.8 | 0.31 | 5.51 |
| Indie | 10,040 | 30,415,027 | 84.02 | 90.12 | 14.37 | 16.7 | 0.40 | 4.82 |
| Quiz/Trivia | 201 | 92,888 | 83.68 | 81.34 | 14.94 | 32.6 | 0.30 | 6.46 |
| Strategy | 5,634 | 17,025,995 | 83.67 | 87.52 | 14.44 | 39.6 | 0.35 | 5.42 |

Point-and-click leads with 1,277 rated games. Puzzle, Platform, Visual Novel, Card & Board Game, Music round out the top six. Shooter, FPS, Hack-and-slash, Fighting do not appear in this top ten. Indie at 10,040 rated games is the largest population on the leaderboard and lands eighth at 84.02.

## Point-and-click, Puzzle, Platform: genre drill-down

Top 3 games per genre. `allGenres` reflects every genre tag the game carries. The Spirit and the Mouse carries 714 reviews, the smallest game in the table. Spirit City: Lofi Sessions' top review was written at 436.7 hours played. Toem's top review carries a negative thumb: the reviewer did not recommend.

*Source: `Q27-genre-game-drilldown.sql`*

| genre | game | text sentiment | reviews | allGenres | influence | upvotes | funny | playtime (h) |
|---|---|---:|---:|---|---:|---:|---:|---:|
| Platform | Smushi Come Home | 96.31 | 2,290 | Adventure, Indie, Platform, Puzzle, RPG | 0.728 | 44 | 4 | 9.7 |
| Platform | Luna's Fishing Garden | 96.18 | 1,358 | Adventure, Indie, Platform, Simulator | 0.668 | 20 | 0 | 4.9 |
| Platform | The Spirit and the Mouse | 96.01 | 714 | Adventure, Indie, Platform, Puzzle | 0.774 | 38 | 5 | 14.9 |
| Point-and-click | Spirit City: Lofi Sessions | 97.01 | 9,499 | Indie, Point-and-click, Simulator | 0.687 | 116 | 2 | 436.7 |
| Point-and-click | Milo and the Magpies | 96.38 | 2,151 | Adventure, Point-and-click, Puzzle | 0.697 | 46 | 2 | 10.3 |
| Point-and-click | Birth | 96.33 | 2,493 | Adventure, Indie, Point-and-click, Puzzle | 0.700 | 60 | 0 | 3.8 |
| Puzzle | Minami Lane | 97.11 | 4,621 | Indie, Puzzle, Simulator, Strategy | 0.750 | 51 | 4 | 8.6 |
| Puzzle | Toem | 96.76 | 3,888 | Adventure, Indie, Puzzle | 0.704 | 51 | 7 | 5.5 |
| Puzzle | The Room VR: A Dark Matter | 96.41 | 2,525 | Adventure, Indie, Puzzle | 0.735 | 66 | 1 | 36.5 |

**Smushi Come Home** (9.7h):

> You're an adorable little mushroom in a colorful open world, helping critters, exploring, recording mushroom species, collecting crystals, and trying to find your way home. All with an absolutely incredible soundtrack that is just soooo relaxing.

**Luna's Fishing Garden** (4.9h, full review):

> This game is amazing. From the gameplay to the lore, this game is for casual and hardcore gamers alike. The lore is extremely rich and deep and it will keep you invested. I recommend this to anyone.

**The Spirit and the Mouse** (14.9h):

> Really wholesome and relaxing game, we all need games like this just to tone it down and chill once in a while. I loved it so much, a perfect before going to sleep game.

**Spirit City: Lofi Sessions** (436.7h at review):

> I use it every day since the purchase, it helps me to be more productive and to relax. It's a perfect relaxing idle game, the best in my opinion, to play in the background while working on something.

**Milo and the Magpies** (10.3h):

> It is an enchanting little game! I loved every single aspect of this fascinating adventure with plenty of poetry, sense of humor and learned references with a beautiful touching ending. I was completely enchanted by the artwork, the humour and the poetry in it.

**Birth** (3.8h):

> How do you even make the "body horror" feel so soothing? Madison is pretty much the only developer I know that can make you poke into flesh to collect organs, while also having a wholesome introspective experience.

**Minami Lane** (8.6h):

> It feels like drinking a glass of hot tea, so comfy! My picky eater kid was having trouble sleeping and was watching me play and kept talking excitedly about the Ramen. To the point where we got up and made some quick ramen at like 11pm!

**Toem** (5.5h, reviewer did not recommend):

> TOEM isn't a bad game, the reason I'm not recommending it is because I just have a hard time finding someone I would actually recommend this to. It's not a true puzzle game, the "puzzles" consist of people either explicitly or vaguely telling you what they want you to take a picture of.

**The Room VR: A Dark Matter** (36.5h, full review):

> THIS is why I bought VR in the first place, for experiences like this, being completely immersed in a fantasy world, following an interesting story and solving well thought out, VR friendly puzzles. I needed the hint system only twice and I'm not great at puzzle games, but these puzzles are so well put together and make complete logical sense. 10/10 Completely recommend!

## The golden zone: where genre and theme meet

The aggregate tables measure theme grain and genre grain separately. A joint ranking of genre x theme combinations, weighted by `sentimentReviews`, surfaces three pockets where both labels score high at the same time.

*Source: `Q28-golden-zone-drilldown.sql`*

| genre | theme | combo games | combo sentiment |
|---|---|---:|---:|
| Strategy | Romance | 76 | 92.45 |
| RPG | Business | 60 | 91.52 |
| Point-and-click | Kids | 96 | 91.22 |

Combo sentiment averages over all games carrying both labels simultaneously, which pulls it below the per-theme and per-genre ceilings in the tables above. The individual games within each combo still reach 93-96.

---

**Strategy x Romance** (combo sentiment 92.45, 76 games)

| game | text sentiment | reviews | allThemes | influence | upvotes | funny | playtime (h) |
|---|---:|---:|---|---:|---:|---:|---:|
| Regency Solitaire | 95.34 | 750 | Historical, Kids, Romance | 0.766 | 127 | 8 | 23.3 |
| Stardew Valley | 93.27 | 130,714 | Business, Fantasy, Romance, Sandbox | 0.704 | 588 | 20 | 191.8 |
| I Was a Teenage Exocolonist | 93.21 | 4,056 | Drama, Romance, Science fiction | 0.719 | 66 | 81 | 48.3 |

**Regency Solitaire** (23.3h):

> Regency Solitaire is a totally charming take on the single-player card game. A variation on the golf solitaire play style, this fast-paced version of the classic game will have you addicted before you can say "tea and biscuits!" Golf solitaire involves playing cards in ascending or descending order until all have been removed from the board.

**Stardew Valley** (191.8h at review):

> There was a night I was ready to disappear. This game distracted me, grounded me, kept my hands busy when my thoughts weren't safe. And somehow I'm still here. I have a girlfriend now. I'm graduating from college this May. A future I once thought I'd never have is suddenly real.

**I Was a Teenage Exocolonist** (48.3h, 81 funny votes, full review):

> Love Anemone Love Cal Love Tangent Love Dys Love Marz Love Tammy Love Sym Love Nomi Love Rex We don't talk about Vace

---

**RPG x Business** (combo sentiment 91.52, 60 games)

| game | text sentiment | reviews | allThemes | influence | upvotes | funny | playtime (h) |
|---|---:|---:|---|---:|---:|---:|---:|
| Tiny Bookshop | 96.13 | 4,690 | Business | 0.864 | 308 | 120 | 58.6 |
| Potionomics | 93.47 | 5,351 | Business, Fantasy | 0.700 | 660 | 16 | 21.2 |
| Story of Seasons: Friends of Mineral Town | 93.41 | 3,496 | Business, Fantasy | 0.820 | 728 | 30 | 75.5 |

**Tiny Bookshop** review in the theme drill-down above.

**Potionomics** (21.2h, 660 upvotes):

> Potionomics is a charmer of a title. It's satisfying, it's interesting, it's compelling. The art and visual design are both utterly delightful, and the fundamental structure of the game is a lovely send-up of other cutesy commerce classics like Recettear: An Item Shop's Tale.

**Story of Seasons: Friends of Mineral Town** (75.5h, 728 upvotes, reviewer did not recommend):

> I know this review is negative, however, most of my gripes with the game are due to it being a 2021 release. I still love the game, putting 75 and a half hours as of writing this in the last week and a half to bust out all the achievements. It's just that there are better games for cheaper.

---

**Point-and-click x Kids** (combo sentiment 91.22, 96 games)

| game | text sentiment | reviews | allThemes | influence | upvotes | funny | playtime (h) |
|---|---:|---:|---|---:|---:|---:|---:|
| Nelly Cootalot: The Fowl Fleet | 95.69 | 231 | Comedy, Kids | 0.808 | 82 | 37 | 10.8 |
| A Castle Full of Cats | 95.55 | 2,556 | Horror, Kids | 0.724 | 68 | 2 | 3.4 |
| Botanicula | 94.84 | 3,049 | Comedy, Fantasy, Kids, Party | 0.763 | 61 | 1 | 13.2 |

**Nelly Cootalot: The Fowl Fleet** (10.8h):

> If you love British humor, I highly recommend this game. If you love humorous games at all, I recommend this. If you love games with charm, I recommend this game. If you love talking birds, I recommend this.

**A Castle Full of Cats** (3.4h):

> After A Building Full of Cats I was eagerly awaiting the sequel and for my part I wasn't disappointed. And for the price you get a whole lot of game. I just wanted to take a quick look at the game during my lunch break in order to play it at home, but how could it be otherwise, nothing came of it and I played through the game almost in one go.

**Botanicula** (13.2h):

> With Botanicula, Amanita Design presents a mixture of adventure, puzzle and search game and work of art in a somewhat strange tree world that is threatened by spider-like creatures. A team of five strange creatures face the threat and set out to save their world.

## The pattern is structural

Three reads of the same metric, three matching shapes. Game grain: cozy and short-narrative titles top the table. Theme grain: Business, Educational, Romance, Kids, Mystery, Drama, Comedy lead. Genre grain: Point-and-click, Puzzle, Platform, Visual Novel, Card & Board Game, Music lead. The labels are different at each grain. The shape is the same: small, story-led, low-friction games sit on top of `weightedSentimentRating`, and the action / shooter / horror clusters that headline the alignment doc's negative tail sit lower on this leaderboard.

## Sources

- Queries: `Q23-top-sentimentrated-games.sql`, `Q24-aggregate-sentiment-leaders.sql` (battery preserved alongside this finding); theme + genre context also in `Q07-theme-alignment.sql`, `Q15-genre-playtime.sql`, `Q16-theme-playtime.sql`
- Drill-down queries: `Q25-game-review-drilldown.sql` (top review per top-3 game), `Q26-theme-game-drilldown.sql` (top 3 games per Business/Educational/Romance theme), `Q27-genre-game-drilldown.sql` (top 3 games per Point-and-click/Puzzle/Platform genre), `Q28-golden-zone-drilldown.sql` (golden zone combos and games)
- Reproduce via: [Labs/Lab03_duckdb_gold/](../../Labs/Lab03_duckdb_gold/) (DuckDB harness over Gold parquet exports)
- Methodology: [scoring-model.md](../architecture/scoring-model.md) (§ *Bayesian shrinkage with empirically-derived priors*, § *Review influence weights*)
- Companion findings: [sentiment-vote-alignment.md](sentiment-vote-alignment.md) (the gap between text and votes), [where-the-gap-grows.md](where-the-gap-grows.md) (how often vote tier and sentiment tier disagree, by review volume)
- Caveats:
  - All ratings are post-shrinkage. The 10,000-review floor on the games table is curatorial: it keeps the top to recognisable / audience-tested titles rather than mid-volume indies the smoothing happens to seat near the ceiling. The `Q23` query parameter can be lowered to widen the population.
  - The 100-rated-games floor on theme / genre tables drops dim members too small to carry an aggregate honestly.
  - Genres / themes pulled from `gold.vw_gameCatalogue`, with `'Unknown'` (IGDB placeholder) stripped. Some IGDB labels abbreviated for table width: "Role-playing (RPG)" → RPG, "Turn-based strategy (TBS)" → TBS, "Science fiction" → Sci-fi, "4X (explore, expand, exploit, and exterminate)" → 4X.
  - Theme / genre aggregates from `gold.vw_aggThemes` and `gold.vw_aggGenres`. The sentiment columns on those views are weighted by `sentimentReviews`; refund / bug / vote columns are weighted by `totalReviews` (`semantic-layer-lite.md` §3).
  - The drill-down review tables pull the single highest-`reviewInfluenceScore` review per game from `isVaderEligible = TRUE` rows, regardless of thumb direction. Two reviews in the drill-down sections carry a negative thumb (Toem, Story of Seasons: Friends of Mineral Town); `reviewInfluenceScore` weighs community engagement, not the thumb.
  - Golden zone combo sentiment is a `sentimentReviews`-weighted average across all games carrying both labels; it is lower than individual game/theme/genre scores by construction.
