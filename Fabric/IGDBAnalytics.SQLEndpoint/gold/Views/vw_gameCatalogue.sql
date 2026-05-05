-- Auto Generated (Do not modify) AC75D62E5A62CA8B74E600506F8ED05C67F2ED4BE9BA5DC9A112BBEA1CF136EE




create   view gold.vw_gameCatalogue as
select distinct
    g.gameKey
    , g.gameName
    , gg.genreName
    , gt.themeName
    , gp.platformType
    , gp.platformName
from gold.vw_dimGames as g
left join gold.vw_gameGenres as gg
    on g.gameKey = gg.gameKey
left join gold.vw_gameThemes as gt
    on g.gameKey = gt.gameKey
left join gold.vw_gamePlatforms as gp
    on g.gameKey = gp.gameKey