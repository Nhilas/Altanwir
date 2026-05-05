# 02_Analytics

> Queries: [02_Analytics_Queries.sql](02_Analytics_Queries.sql)

## Screenshots

| Filename | Description |
|---|---|
| A01_factGameScores_top20.png | Top 20 games by overall score from factGameScores. |
| A02_alignment_negative_top10.png | Top 10 games where critic / user sentiment alignment is most negative (biggest disagreement, skewed bad). |
| A03_alignment_positive_top10.png | Top 10 games where critic / user alignment is most positive. |
| A04_aggGenres.png | Aggregated metrics by genre. |
| A05_aggThemes.png | Aggregated metrics by theme. |
| A06_tier_distribution.png | How games distribute across score tiers. |
| A07_insufficient_data.png | Games filtered out because they don't have enough reviews to score reliably. |
| A08_volume_vs_quality.png | Scatter / chart of review volume vs average quality. |
| A09_games_detail.png | Per-game detail view (drill-down). |
| A010_top5_reviews_of_top5_games_sentiment.png | Top 5 reviews (by helpfulness) for the top 5 games, colored by sentiment. |
| A011_top5_funny_reviews_of_top5_funniest_games.png | Top 5 funny-flagged reviews from the 5 funniest games (Steam has a "funny" vote). |
| A020_top5_reviews_of_top5_games_sentiment_misaligned.png | Same as A010 but filtered to reviews where sentiment disagrees with score. |
| A030_top5_reviews_of_top5_games_sentiment_aligned.png | Same as A010 but filtered to reviews where sentiment matches score. |
| A040_top5_games_in_top5_genres.png | Top 5 games inside each of the top 5 genres. |
| A050_top5_games_in_top5_themes.png | Top 5 games inside each of the top 5 themes. |

---

## View Exports

| Filename | Contents |
|---|---|
| vw_aggGenres.xlsx | Per-genre rollup: counts, average scores, sentiment metrics (the data behind A04_aggGenres.png). |
| vw_aggPlatforms.xlsx | Per-platform rollup: same metrics broken out by platform (PC / PS5 / Xbox / Switch / etc). |
| vw_aggThemes.xlsx | Per-theme rollup (the data behind A05_aggThemes.png). |
| vw_factGameScores.xlsx | Game-level scores—one row per game with overall score, sentiment, tier, review count, etc. (the data behind A01_factGameScores_top20.png and A06_tier_distribution.png). |
