-- Auto Generated (Do not modify) 9E0B7D1607EC8B77A98780DA32CBFB4FADD74AE6044D4E7EF940F16632225D51
create   view gold.vw_aggThemes
as
with agg_scores as (
    select
        gt.themeName
        , sum(case when smoothedIGDBRating is not null then 1 else 0 end)                            as ratedGames
        , sum(smoothedIGDBRating * IGDBSourceCount)        / nullif(sum(IGDBSourceCount), 0)         as weightedIGDBRating
        , sum(IGDBSourceCount)                                                                       as IGDBSourceCount
        , sum(case when totalReviews is not null then 1 else 0 end)                                  as reviewedGames
        , sum(totalReviews)                                                                          as totalReviews
        , sum(sentimentReviews)                                                                      as sentimentReviews
        , avg(avgPlaytimeAtReview)                                                                   as avgPlaytimeAtReview
        , sum(pctPositiveSentiment * sentimentReviews)     / nullif(sum(sentimentReviews), 0)        as pctPositiveSentiment
        , sum(pctNeutralSentiment * sentimentReviews)      / nullif(sum(sentimentReviews), 0)        as pctNeutralSentiment
        , sum(pctNegativeSentiment * sentimentReviews)     / nullif(sum(sentimentReviews), 0)        as pctNegativeSentiment
        , sum(pctEarlyAccess * totalReviews)               / nullif(sum(totalReviews), 0)            as pctEarlyAccess
        , sum(pctBugReports * totalReviews)                / nullif(sum(totalReviews), 0)            as pctBugReports
        , sum(pctRefunded * totalReviews)                  / nullif(sum(totalReviews), 0)            as pctRefunded
        , sum(sentimentVoteAlignment * sentimentReviews)   / nullif(sum(sentimentReviews), 0)        as sentimentVoteAlignment
        , sum(weightedSentimentRating * sentimentReviews)  / nullif(sum(sentimentReviews), 0)        as weightedSentimentRating
        , sum(weightedVoteRating * totalReviews)           / nullif(sum(totalReviews), 0)            as weightedVoteRating
        , sum(voteRating * totalReviews)                   / nullif(sum(totalReviews), 0)            as steamVoteRating
    from gold.factgamescores as gs
    inner join gold.vw_gameThemes as gt
        on gs.gameKey = gt.gameKey
    where gt.themeName <> 'Unknown'        
    group by
        gt.themeName
)

, cast_agg_scores as (
    select
        themeName
        , ratedGames
        , round(weightedIGDBRating * 100, 2)                          as weightedIGDBRating
        , round(IGDBSourceCount, 2)                                   as IGDBSourceCount

        , reviewedGames
        , totalReviews
        , sentimentReviews

        , round(avgPlaytimeAtReview / 60, 2)                          as avgPlaytimeAtReviewHours

        , round(pctPositiveSentiment * 100, 2)                        as pctPositiveSentiment
        , round(pctNeutralSentiment * 100, 2)                         as pctNeutralSentiment
        , round(pctNegativeSentiment * 100, 2)                        as pctNegativeSentiment

        , round(pctEarlyAccess * 100, 2)                              as pctEarlyAccess
        , round(pctBugReports * 100, 2)                               as pctBugReports
        , round(pctRefunded * 100, 2)                                 as pctRefunded

        , round(sentimentVoteAlignment * 100, 2)                      as sentimentVoteAlignment
        , round(weightedSentimentRating * 100, 2)                     as weightedSentimentRating
        , round(weightedVoteRating * 100, 2)                          as weightedVoteRating
        , round(steamVoteRating * 100, 2)                             as steamVoteRating
    from agg_scores
)

select
    themeName
    , ratedGames
    , weightedIGDBRating
    , IGDBSourceCount
    , reviewedGames    
    , totalReviews
    , sentimentReviews
    , avgPlaytimeAtReviewHours
    , pctPositiveSentiment
    , pctNeutralSentiment
    , pctNegativeSentiment
    , sentimentVoteAlignment
    , weightedSentimentRating
    , weightedVoteRating
    , steamVoteRating
    , pctRefunded
    , pctBugReports
    , pctEarlyAccess
from cast_agg_scores