with stgGames as (
    select
        gameId
        , gameName
        , releaseDate
        , estimatedOwners
        , peakCCU
        , requiredAge
        , price
        , supportedLanguages
        , fullAudioLanguages
        , website
        , platform
        , achievements
        , recommendations
        , notes
        , averagePlaytimeForever
        , medianPlaytimeForever
        , developers
        , publishers
        , categories
        , genres
        , tags
    from {{ ref('stgGames') }}
)

select
    *
from stgGames