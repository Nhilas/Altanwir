with rawReviews as (
    {% if target=='prod' %}
        select * from {{ source('fabricSteam', 'reviews') }}
    {% else %}
        select * from {{ source('localSteam', 'reviews') }}
    {% endif %}    
        where review_text is not null
            and ltrim(rtrim(review_text)) not in ('', '-') 
            and app_id is not null
)

, ranked as (
    select
        app_id
        , ltrim(rtrim(review_text)) as review_text
        , review_score
        , review_votes
        , row_number() over (partition by app_id, ltrim(rtrim(review_text)) order by app_id desc) as rn
    from rawReviews
)

, renamed as (
    select
        md5(cast(concat(app_id,review_text,review_score,review_votes) as varchar)) as reviewId  -- hash of the review to create a unique identifier
        , app_id as gameId
        , review_text as reviewText
        , case when review_score = 1 then true else false end as isPositive
        , case when review_votes = 1 then true else false end as isVoted
    from ranked
    where rn = 1
)

select *
from renamed