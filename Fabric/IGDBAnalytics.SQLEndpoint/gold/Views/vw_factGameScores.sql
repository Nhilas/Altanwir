-- Auto Generated (Do not modify) B8C43859BD35C42EDF697928943785433AF44C204E457C0FF40F8E436E36CA74
create view gold.vw_factGameScores
as
with cast_game_scores as (
    select
        gameKey
        , round(pctIGDBRating * 100, 2) as IGDBRating
        , round(smoothedIGDBRating * 100, 2) as smoothedIGDBRating
        , IGDBSourceCount

        , totalReviews
        , sentimentReviews

        , round(avgPlaytimeAtReview / 60, 2) as avgPlaytimeAtReviewHours
        , cast(round(avgWordCount, 0) as int) as avgWordCount
        , round(avgEmotionalIntensity * 100, 2) as avgEmotionalIntensity

        , round(pctPositiveSentiment * 100, 2) as pctPositiveSentiment
        , round(pctNeutralSentiment * 100, 2) as pctNeutralSentiment
        , round(pctNegativeSentiment * 100, 2) as pctNegativeSentiment

        , round(pctEarlyAccess * 100, 2) as pctEarlyAccess
        , round(pctBugReports * 100, 2) as pctBugReports
        , round(pctRefunded * 100, 2) as pctRefunded

        , round(sentimentVoteAlignment * 100, 2) as sentimentVoteAlignment
        , round(weightedSentimentRating * 100, 2) as weightedSentimentRating
        , round(weightedVoteRating * 100, 2) as weightedVoteRating
        , round(voteRating * 100, 2) as steamVoteRating
    from gold.factgamescores
)

, tiered_game_scores as (
    select
        *
        , case
            when smoothedIGDBRating is not null
                then
                    case
                        when IGDBSourceCount < 10 then 'Insufficient Data'
                        when smoothedIGDBRating >= 95 then 'S'
                        when smoothedIGDBRating >= 87 then 'A'
                        when smoothedIGDBRating >= 78 then 'B'
                        when smoothedIGDBRating >= 68 then 'C'
                        when smoothedIGDBRating >= 55 then 'D'
                        else 'F'
                    end
        end as IGDBRatingTier
        , case
            when weightedSentimentRating is not null
                then
                    case
                        when sentimentReviews < 10 then 'Insufficient Data'
                        when weightedSentimentRating >= 95 then 'S'
                        when weightedSentimentRating >= 87 then 'A'
                        when weightedSentimentRating >= 78 then 'B'
                        when weightedSentimentRating >= 68 then 'C'
                        when weightedSentimentRating >= 55 then 'D'
                        else 'F'
                    end
        end as weightedSentimentTier
        , case
            when weightedVoteRating is not null
                then
                    case
                        when totalReviews < 10 then 'Insufficient Data'
                        when weightedVoteRating >= 95 then 'S'
                        when weightedVoteRating >= 87 then 'A'
                        when weightedVoteRating >= 78 then 'B'
                        when weightedVoteRating >= 68 then 'C'
                        when weightedVoteRating >= 55 then 'D'
                        else 'F'
                    end
        end as weightedVoteTier
        , case
            when totalReviews >= 500
                then
                    case
                        when steamVoteRating >= 95 then 'Overwhelmingly Positive'
                        when steamVoteRating between 80 and 94.99 then 'Very Positive'
                        when steamVoteRating between 70 and 79.99 then 'Mostly Positive'
                        when steamVoteRating between 40 and 69.99 then 'Mixed'
                        when steamVoteRating between 20 and 39.99 then 'Mostly Negative'
                        when steamVoteRating < 20 then 'Overwhelmingly Negative'
                    end
            when totalReviews between 50 and 499
                then
                    case
                        when steamVoteRating >= 80 then 'Very Positive'
                        when steamVoteRating between 70 and 79.99 then 'Mostly Positive'
                        when steamVoteRating between 40 and 69.99 then 'Mixed'
                        when steamVoteRating between 20 and 39.99 then 'Mostly Negative'
                        when steamVoteRating < 20 then 'Very Negative'
                    end
            when totalReviews between 10 and 49
                then
                    case
                        when steamVoteRating >= 80 then 'Positive'
                        when steamVoteRating between 70 and 79.99 then 'Mostly Positive'
                        when steamVoteRating between 40 and 69.99 then 'Mixed'
                        when steamVoteRating between 20 and 39.99 then 'Mostly Negative'
                        when steamVoteRating < 20 then 'Negative'
                    end
            when totalReviews < 10 then 'Insufficient Data'
        end as steamRatingLabel
    from cast_game_scores
)

select
    gs.gameKey
    , g.gameName
    , IGDBRating
    , smoothedIGDBRating
    , IGDBRatingTier
    , IGDBSourceCount
    , totalReviews
    , sentimentReviews
    , avgPlaytimeAtReviewHours
    , avgWordCount
    , avgEmotionalIntensity
    , pctPositiveSentiment
    , pctNeutralSentiment
    , pctNegativeSentiment
    , sentimentVoteAlignment
    , weightedSentimentRating
    , weightedSentimentTier
    , weightedVoteRating
    , weightedVoteTier
    , steamVoteRating
    , steamRatingLabel
    , pctRefunded
    , pctBugReports
    , pctEarlyAccess
from tiered_game_scores as gs
inner join gold.vw_dimGames as g
    on gs.gameKey = g.gameKey
