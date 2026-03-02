with stgReviews as (
    select
        gameId
        , reviewText
        , isPositive
        , isVoted
    from {{ ref('stgReviews') }}
)

select
    *
from stgReviews