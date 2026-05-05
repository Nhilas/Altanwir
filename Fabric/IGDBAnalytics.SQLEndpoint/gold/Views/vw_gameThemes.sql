-- Auto Generated (Do not modify) 85D7E6EBA8F9F83C4B4035F8C1D59122FB67E1D74EAEC53B5558B6D3EFF145E3



create view gold.vw_gameThemes as
with themes as (
    select distinct
        bt.gameKey
        , t.themeName
    from silver.bridgegamethemes as bt
    inner join gold.vw_dimTheme as t
        on bt.themeKey = t.themeKey
)
select
    g.gameKey
    , coalesce(t.themeName, 'Unknown') as themeName
from gold.vw_dimGames as g
left join themes as t
    on g.gameKey = t.gameKey
