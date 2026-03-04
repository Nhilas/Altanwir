select
    games.gameName
    , games.releaseDate
    , games.estimatedOwners
    , games.peakCCU
    , games.requiredAge
    , games.price
    , games.supportedLanguages
    , games.fullAudioLanguages
    , games.website
    , games.platform
    , games.achievements
    , games.recommendations
    , games.notes
    , games.averagePlaytimeForever
    , games.medianPlaytimeForever
    , games.developers
    , games.publishers
    , games.categories
    , games.genres
    , games.tags    
    , reviews.reviewText
    , reviews.isPositive
    , reviews.isVoted
from {{ ref('fctReviews') }} as reviews
join {{ ref('dimGames') }} as games
    on reviews.gameId = games.gameId