-- Auto Generated (Do not modify) 02C0E75254054A7291D1816CF392EE481CE910958D6A31D9D13DF003FA8569CE
create view gold.vw_gameGenres as
with genres as (
    select distinct
        bg.gameKey
        , g.genreName
    from silver.bridgegamegenres as bg
    inner join gold.vw_dimGenre as g
        on bg.genreKey = g.genreKey
)
select
    g.gameKey
    , coalesce(ge.genreName, 'Unknown') as genreName
from gold.vw_dimGames as g
left join genres as ge
    on g.gameKey = ge.gameKey
