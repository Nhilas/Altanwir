-- A01_factGameScores_top20

SELECT TOP 20
    gameKey,
    gameName,
    IGDBRating,
    smoothedIGDBRating,
    IGDBRatingTier,
    IGDBSourceCount,
    totalReviews,
    sentimentReviews,
    steamVoteRating,
    weightedVoteRating,
    weightedSentimentRating,
    steamRatingLabel,
    weightedVoteTier,
    weightedSentimentTier,
    sentimentVoteAlignment
FROM gold.vw_factGameScores
WHERE totalReviews >= 1000
ORDER BY weightedSentimentRating DESC;

-- A010_top5_reviews_of_top5_games_sentiment

;WITH top_5_games AS (
    SELECT TOP 5 gameKey
        , row_number() OVER (ORDER BY weightedSentimentRating DESC ) AS game_rank
    FROM gold.vw_factGameScores
    WHERE totalReviews >= 1000
    ORDER BY weightedSentimentRating DESC
)

, ranked_reviews AS (
    SELECT reviewKey
        , tg.gameKey
        , tg.game_rank
        , row_number() OVER (PARTITION BY r.gameKey ORDER BY reviewInfluenceScore DESC) AS review_rank
    FROM gold.vw_factReviews AS r
    INNER JOIN top_5_games AS tg
        ON r.gameKey = tg.gameKey
)
SELECT
    r.gameName
    , r.reviewCleaned
    , r.votesUp
    , r.votesFunny
    , r.commentCount
    , r.reactionCount
    , r.playtimeAtReview as playtimeMinutes
    , r.playtimeBucket
    , r.sentimentCompound
    , r.sentimentLabel
FROM ranked_reviews AS rr
INNER JOIN gold.vw_factReviews AS r
    ON rr.gameKey = r.gameKey
    AND rr.reviewKey = r.reviewKey
WHERE rr.review_rank <= 5
ORDER BY game_rank, review_rank

--  A020_alignment_negative_top10

SELECT TOP 10
    gameKey,
    gameName,
    IGDBRating,
    smoothedIGDBRating,
    IGDBRatingTier,
    IGDBSourceCount,
    totalReviews,
    steamVoteRating,
    weightedVoteRating,
    weightedSentimentRating,
    sentimentVoteAlignment
FROM gold.vw_factGameScores
WHERE totalReviews >= 50000
ORDER BY sentimentVoteAlignment ASC;

--  A020_top5_reviews_of_top5_games_sentiment_misaligned

;WITH top_5_games AS (
    SELECT TOP 5 gameKey
        , sentimentVoteAlignment
        , row_number() OVER (ORDER BY sentimentVoteAlignment asc ) AS game_rank
    FROM gold.vw_factGameScores
    WHERE totalReviews >= 1000
    ORDER BY sentimentVoteAlignment asc
)

, ranked_reviews AS (
    SELECT reviewKey
        , tg.gameKey
        , tg.sentimentVoteAlignment
        , tg.game_rank
        , row_number() OVER (PARTITION BY r.gameKey ORDER BY reviewInfluenceScore desc) AS review_rank
    FROM gold.vw_factReviews AS r
    INNER JOIN top_5_games AS tg
        ON r.gameKey = tg.gameKey
    where
        sign(r.sentimentCompound) <> r.voteDirection
)
SELECT
    r.gameName
    , r.reviewCleaned
    , r.votedUp
    , r.votesUp
    , r.votesFunny
    , r.commentCount
    , r.reactionCount
    , r.playtimeAtReview as playtimeMinutes
    , r.playtimeBucket
    , r.sentimentCompound
    , r.sentimentLabel
    , r.containsBugReport
    , r.refunded
FROM ranked_reviews AS rr
INNER JOIN gold.vw_factReviews AS r
    ON rr.gameKey = r.gameKey
    AND rr.reviewKey = r.reviewKey
WHERE rr.review_rank <= 5
ORDER BY game_rank, review_rank


--  A03_alignment_positive_top10

SELECT TOP 10
    gameKey,
    gameName,
    IGDBRating,
    smoothedIGDBRating,
    IGDBRatingTier,
    IGDBSourceCount,
    totalReviews,
    steamVoteRating,
    weightedVoteRating,
    weightedSentimentRating,
    sentimentVoteAlignment,
    pctBugReports
FROM gold.vw_factGameScores
WHERE totalReviews >= 50000
ORDER BY sentimentVoteAlignment DESC;

--  A030_top5_reviews_of_top5_games_sentiment_aligned

;WITH top_5_games AS (
    SELECT TOP 5 gameKey
        , sentimentVoteAlignment
        , row_number() OVER (ORDER BY sentimentVoteAlignment asc ) AS game_rank
    FROM gold.vw_factGameScores
    WHERE totalReviews >= 1000
    ORDER BY sentimentVoteAlignment desc
)
, ranked_reviews AS (
    SELECT reviewKey
        , tg.gameKey
        , tg.sentimentVoteAlignment
        , tg.game_rank
        , row_number() OVER (PARTITION BY r.gameKey ORDER BY reviewInfluenceScore desc) AS review_rank
    FROM gold.vw_factReviews AS r
    INNER JOIN top_5_games AS tg
        ON r.gameKey = tg.gameKey
    where
        sign(r.sentimentCompound) = r.voteDirection
)
SELECT
    r.gameName
    , r.reviewCleaned
    , r.votedUp
    , r.votesUp
    , r.votesFunny
    , r.commentCount
    , r.reactionCount
    , r.playtimeAtReview as playtimeMinutes
    , r.playtimeBucket
    , r.sentimentCompound
    , r.sentimentLabel
    , r.containsBugReport
    , r.refunded
FROM ranked_reviews AS rr
INNER JOIN gold.vw_factReviews AS r
    ON rr.gameKey = r.gameKey
    AND rr.reviewKey = r.reviewKey
WHERE rr.review_rank <= 5
ORDER BY game_rank, review_rank

-- A011_top5_funny_reviews_of_top5_funniest_games

;WITH average_funny_votes as (
    select top 5 gameKey
        , avg(votesFunny) as avg_votesFunny
        , count(distinct(reviewKey)) as total_reviews
        , row_number() over ( order by avg(votesFunny) desc ) as game_rank
    from gold.vw_factReviews
    group by gameKey
    having count(distinct(reviewKey)) >= 1000
    order by avg_votesFunny desc
)

, ranked_reviews AS (
    SELECT reviewKey
        , tg.gameKey
        , tg.game_rank
        , row_number() OVER (PARTITION BY r.gameKey ORDER BY votesFunny desc) AS review_rank
    FROM gold.vw_factReviews AS r
    INNER JOIN average_funny_votes AS tg
        ON r.gameKey = tg.gameKey
)
SELECT
    r.gameName
    , r.reviewCleaned
    , r.votedUp
    , r.votesFunny
    , r.sentimentLabel
FROM ranked_reviews AS rr
INNER JOIN gold.vw_factReviews AS r
    ON rr.gameKey = r.gameKey
    AND rr.reviewKey = r.reviewKey
WHERE rr.review_rank <= 5
ORDER BY game_rank, review_rank



-- A04_aggGenres
SELECT
    genreName,
    ratedGames,
    weightedIGDBRating,
    reviewedGames,
    weightedSentimentRating,
    weightedVoteRating,
    sentimentVoteAlignment
FROM gold.vw_aggGenres
WHERE reviewedGames >= 20
ORDER BY weightedSentimentRating DESC

-- A040_top5_games_in_top5_genres

with top_5_themes as (
    select top 5
        genreName
        , row_number() OVER (ORDER BY weightedSentimentRating desc ) AS genre_rank
    from gold.vw_aggGenres
    order by weightedSentimentRating desc
)

, genre_games as (
    select distinct t.genreName, t.genre_rank, gc.gameKey
    from top_5_themes as t
    inner join gold.vw_gameCatalogue as gc
        on t.genreName = gc.genreName
)

, top_5_games as (
    select
        tg.genreName
        , tg.genre_rank
        , gs.gameKey
        , row_number() OVER (PARTITION BY tg.genreName ORDER BY weightedSentimentRating desc) AS game_rank
    from gold.vw_factGameScores as gs
    inner join genre_games as tg
        on gs.gameKey = tg.gameKey
)

select
    tg.genreName,
    gs.gameName,
    gs.IGDBRating,
    gs.smoothedIGDBRating,
    gs.IGDBSourceCount,
    gs.totalReviews,
    gs.steamVoteRating,
    gs.weightedVoteRating,
    gs.weightedSentimentRating,
    gs.sentimentVoteAlignment,
    gs.pctBugReports
from gold.vw_factGameScores as gs
inner join top_5_games as tg
    on gs.gameKey = tg.gameKey
where tg.game_rank <= 5
order by tg.genre_rank, tg.game_rank

-- A05_aggThemes
SELECT
    themeName,
    ratedGames,
    weightedIGDBRating,
    reviewedGames,
    weightedSentimentRating,
    weightedVoteRating,
    sentimentVoteAlignment
FROM gold.vw_aggThemes
WHERE reviewedGames >= 20
ORDER BY weightedSentimentRating DESC

-- A050_top5_games_in_top5_themes

with top_5_themes as (
    select top 5
        themeName
        , row_number() OVER (ORDER BY weightedSentimentRating desc ) AS theme_rank
    from gold.vw_aggThemes
    order by weightedSentimentRating desc
)

, theme_games as (
    select distinct t.themeName, t.theme_rank, gc.gameKey
    from top_5_themes as t
    inner join gold.vw_gameCatalogue as gc
        on t.themeName = gc.themeName
)

, top_5_games as (
    select
        tg.themeName
        , tg.theme_rank
        , gs.gameKey
        , row_number() OVER (PARTITION BY tg.themeName ORDER BY weightedSentimentRating desc) AS game_rank
    from gold.vw_factGameScores as gs
    inner join theme_games as tg
        on gs.gameKey = tg.gameKey
)

select
    tg.themeName,
    gs.gameName,
    gs.IGDBRating,
    gs.smoothedIGDBRating,
    gs.IGDBSourceCount,
    gs.totalReviews,
    gs.steamVoteRating,
    gs.weightedVoteRating,
    gs.weightedSentimentRating,
    gs.sentimentVoteAlignment,
    gs.pctBugReports
from gold.vw_factGameScores as gs
inner join top_5_games as tg
    on gs.gameKey = tg.gameKey
where tg.game_rank <= 5
order by tg.theme_rank, tg.game_rank

-- A06_tier_distribution
SELECT
    weightedSentimentTier,
    COUNT(*) AS games,
    AVG(weightedSentimentRating) AS avg_sentiment,
    MIN(weightedSentimentRating) AS min_sentiment,
    MAX(weightedSentimentRating) AS max_sentiment
FROM gold.vw_factGameScores
WHERE weightedSentimentTier <> 'Insufficient Data'
GROUP BY weightedSentimentTier
ORDER BY MIN(weightedSentimentRating) DESC;


-- A07_insufficient_data
SELECT
    CASE WHEN totalReviews < 10 THEN 'Insufficient' ELSE 'Sufficient' END AS bucket,
    COUNT(*) AS games,
    MIN(totalReviews) AS min_reviews,
    MAX(totalReviews) AS max_reviews
FROM gold.factgamescores
GROUP BY CASE WHEN totalReviews < 10 THEN 'Insufficient' ELSE 'Sufficient' END;


-- A08_volume_vs_quality
SELECT
    CASE
        WHEN totalReviews < 100 THEN '0-99'
        WHEN totalReviews < 1000 THEN '100-999'
        WHEN totalReviews < 10000 THEN '1k-10k'
        WHEN totalReviews < 100000 THEN '10k-100k'
        ELSE '100k+'
    END AS review_volume_bucket,
    COUNT(*) AS games,
    AVG(weightedSentimentRating) AS avg_sentiment_rating,
    AVG(weightedVoteRating) AS avg_vote_rating
FROM gold.factgamescores
WHERE totalReviews >= 10
GROUP BY CASE
        WHEN totalReviews < 100 THEN '0-99'
        WHEN totalReviews < 1000 THEN '100-999'
        WHEN totalReviews < 10000 THEN '1k-10k'
        WHEN totalReviews < 100000 THEN '10k-100k'
        ELSE '100k+'
    END
ORDER BY MIN(totalReviews);

-- A09_games_detail
SELECT *
FROM gold.vw_factGameScores
WHERE gameName IN ( 'Cyberpunk 2077', 'Stellaris', 'Crusader Kings III', 'Baldur''s Gate III', 'Portal 2', 'Starfield', 'Minami Lane', 'Stardew Valley', 'Satisfactory' )
ORDER BY gameName;
