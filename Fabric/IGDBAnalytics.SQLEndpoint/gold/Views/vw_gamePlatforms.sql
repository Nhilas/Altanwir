-- Auto Generated (Do not modify) 809EA76B3BA99BA3B2065249801CCD6A7DDEC678F6CE706EDB5F0613A48B9799




create   view gold.vw_gamePlatforms as
with platforms as (
    select distinct
        bp.gameKey
        , p.platformType
        , p.platformName
    from silver.bridgegameplatforms as bp
    inner join gold.vw_dimPlatform as p
        on bp.platformKey = p.platformKey
)
select
    g.gameKey
    , coalesce(p.platformType, 'Unknown') as platformType
    , coalesce(p.platformName, 'Unknown' ) as platformName
from gold.vw_dimGames as g
left join platforms as p
    on g.gameKey = p.gameKey