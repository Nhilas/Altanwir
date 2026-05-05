-- Auto Generated (Do not modify) D27E675C776371D3C51748EFC1A040F224E1E90E7E4C9EEF38D65E75D48BFCE7



create view gold.vw_dimGames as
select
    gameKey
    , gameName
    , releasedOn
from silver.games
