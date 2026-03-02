with rawReviews as (
    select *
    from {{ source('localSteam', 'reviews') }}
)

, renamed as (
    select
        app_id as gameId
        , review_text as reviewText
        , case when review_score = 1 then true else false end as isPositive
        , case when review_votes = 1 then true else false end as isVoted
    from rawReviews
)

select *
from renamed