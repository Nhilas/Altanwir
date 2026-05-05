-- Auto Generated (Do not modify) 3009F99104971AE4C1F5DD9B749702F05C92B50497ECA91CCE298755CD88F672
create view gold.vw_factReviews
as
with cast_reviews as (
    select
        reviewKey
        , r.gameKey
        , g.gameName

        , reviewCleaned
        , votedUp
        , votesUp
        , votesFunny
        , commentCount
        , reactionCount
        , round(communitySignal, 4) as communitySignal

        , reviewLength
        , wordCount
        , round(wordLengthRatio, 4) as wordLengthRatio
        , round(uniqueWordRatio, 4) as uniqueWordRatio
        , hasCredibleText
        , round(lengthSignal, 4) as lengthSignal

        , playtimeAtReview
        , round(playtimeSignal, 4) as playtimeSignal

        , isVaderEligible
        , sentimentCompound
        , round(sentimentSignal, 4) as sentimentSignal
        , sentimentDirection

        , round(emotionalIntensity, 4) as emotionalIntensity
        , round(emotionalSignal, 4) as emotionalSignal

        , voteDirection
        , round(reviewInfluenceScore, 4) as reviewInfluenceScore
        , round(steamWeightedVoteScore, 4) as steamWeightedVoteScore

        , refunded
        , writtenDuringEarlyAccess
        , containsBugReport
    from gold.factreviews as r
    inner join gold.vw_dimGames as g
        on r.gameKey = g.gameKey
)

, labeled_reviews as (
    select
        *
        , case
            when playtimeSignal >= 0.67 then 'Hardcore'
            when playtimeSignal >= 0.34 then 'Regular'
            when playtimeSignal < 0.34 then 'Casual'
        end as playtimeBucket
        , case
            when sentimentCompound is not NULL
                then
                    case
                        when sentimentCompound >= 0.05 then 'Positive'
                        when sentimentCompound <= -0.05 then 'Negative'
                        else 'Neutral'
                    end
        end as sentimentLabel
    from cast_reviews
)

select
    reviewKey
    , gameKey
    , gameName

    , reviewCleaned
    , votedUp
    , votesUp
    , votesFunny
    , commentCount
    , reactionCount
    , communitySignal

    , reviewLength
    , wordCount
    , wordLengthRatio
    , uniqueWordRatio
    , hasCredibleText
    , lengthSignal

    , playtimeAtReview
    , playtimeSignal
    , playtimeBucket

    , isVaderEligible
    , sentimentCompound
    , sentimentSignal
    , sentimentLabel

    , emotionalIntensity
    , emotionalSignal

    , voteDirection
    , reviewInfluenceScore
    , steamWeightedVoteScore

    , refunded
    , writtenDuringEarlyAccess
    , containsBugReport
from labeled_reviews
