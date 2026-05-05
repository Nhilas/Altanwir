# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "21686009-3b8b-4dac-a144-e9cf00d8b9cc",
# META       "default_lakehouse_name": "IGDBAnalytics",
# META       "default_lakehouse_workspace_id": "d1206eb3-2259-44b8-844a-409f1a63f284",
# META       "known_lakehouses": [
# META         {
# META           "id": "21686009-3b8b-4dac-a144-e9cf00d8b9cc"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Loading OBT gamingAnalytics

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC use gold;
# MAGIC 
# MAGIC with cte_gameGenres as (
# MAGIC         select distinct
# MAGIC             bgg.gameKey
# MAGIC             , gen.genreKey
# MAGIC             , gen.genreName
# MAGIC         from silver.bridgegamegenres as bgg
# MAGIC         inner join silver.genres as gen
# MAGIC             on bgg.genreKey = gen.genreKey
# MAGIC     )
# MAGIC 
# MAGIC , cte_gameThemes as (
# MAGIC         select distinct
# MAGIC             bgt.gameKey
# MAGIC             , t.themeKey
# MAGIC             , t.themeName
# MAGIC         from silver.bridgegamethemes as bgt
# MAGIC         inner join silver.themes as t
# MAGIC             on bgt.themeKey = t.themeKey
# MAGIC     )
# MAGIC 
# MAGIC , cte_gamePlatforms as (    
# MAGIC         select distinct
# MAGIC             bgp.gameKey
# MAGIC             , p.platformKey
# MAGIC             , p.platformName
# MAGIC             , p.platformType
# MAGIC         from silver.bridgegameplatforms as bgp
# MAGIC         inner join silver.platforms as p
# MAGIC             on bgp.platformKey = p.platformKey
# MAGIC     )
# MAGIC 
# MAGIC , cte_gameRatings as (
# MAGIC         select
# MAGIC             gameKey
# MAGIC             , gameName
# MAGIC             , aggregatedRatingSourceCount
# MAGIC             , round(percent_rank() over (order by aggregatedRatingSourceCount),2)*100 as percentileSourceCount    
# MAGIC             , aggregatedRating
# MAGIC             , round(percent_rank() over (order by aggregatedRating),2)*100 as percentileRating
# MAGIC         from silver.games
# MAGIC         where
# MAGIC             aggregatedRating > 0
# MAGIC             and aggregatedRatingSourceCount > 0    
# MAGIC     )
# MAGIC 
# MAGIC , cte_tierCalculation as (
# MAGIC           select
# MAGIC             gameKey
# MAGIC             , gameName
# MAGIC             , aggregatedRating
# MAGIC             , percentileRating
# MAGIC             , case
# MAGIC                 when percentileRating = 100 then 'SSS'
# MAGIC                 when percentileRating = 99 then 'SS'
# MAGIC                 when percentileRating between 95 and 98 then 'S'
# MAGIC                 when percentileRating between 90 and 94 then 'A'
# MAGIC                 when percentileRating between 80 and 89 then 'B'
# MAGIC                 when percentileRating between 70 and 79 then 'C'
# MAGIC                 when percentileRating between 60 and 69 then 'D'
# MAGIC             else 'F' end as ratingTier
# MAGIC             , aggregatedRatingSourceCount
# MAGIC             , percentileSourceCount
# MAGIC             , case 
# MAGIC                 when percentileSourceCount = 100 then 'SSS'
# MAGIC                 when percentileSourceCount = 99 then 'SS'
# MAGIC                 when percentileSourceCount between 95 and 98 then 'S'
# MAGIC                 when percentileSourceCount between 90 and 94 then 'A'
# MAGIC                 when percentileSourceCount between 80 and 89 then 'B'
# MAGIC                 when percentileSourceCount between 70 and 79 then 'C'
# MAGIC                 when percentileSourceCount between 60 and 69 then 'D'
# MAGIC             else 'F' end as sourceCountTier            
# MAGIC         from cte_gameRatings
# MAGIC     )
# MAGIC 
# MAGIC , obt_gamingAnalytics as (
# MAGIC         select
# MAGIC             g.gameKey
# MAGIC             , g.gameName
# MAGIC             , g.aggregatedRating
# MAGIC             , g.percentileRating
# MAGIC             , g.ratingTier
# MAGIC             , g.aggregatedRatingSourceCount
# MAGIC             , g.percentileSourceCount
# MAGIC             , g.sourceCountTier            
# MAGIC             , gen.genreKey
# MAGIC             , gen.genreName
# MAGIC             , p.platformKey
# MAGIC             , p.platformName
# MAGIC             , p.platformType
# MAGIC             , t.themeKey
# MAGIC             , t.themeName
# MAGIC             , sha2(concat_ws(','
# MAGIC                 , g.gameName
# MAGIC                 , cast(g.aggregatedRating as string)
# MAGIC                 , cast(g.percentileRating as string)
# MAGIC                 , g.ratingTier
# MAGIC                 , cast(g.aggregatedRatingSourceCount as string)
# MAGIC                 , cast(g.percentileSourceCount as string)
# MAGIC                 , g.sourceCountTier
# MAGIC                 , gen.genreName
# MAGIC                 , p.platformName
# MAGIC                 , p.platformType
# MAGIC                 , t.themeName ), 256) as hash
# MAGIC         from cte_tierCalculation  as g
# MAGIC         left join cte_gameGenres as gen
# MAGIC             on g.gameKey = gen.gameKey
# MAGIC         left join cte_gamePlatforms as p
# MAGIC             on g.gameKey = p.gameKey
# MAGIC         left join cte_gameThemes as t
# MAGIC             on g.gameKey = t.gameKey
# MAGIC     )
# MAGIC 
# MAGIC merge into gold.gaminganalytics as t
# MAGIC using obt_gamingAnalytics as s
# MAGIC     on t.gameKey = s.gameKey
# MAGIC     and coalesce(t.genreKey, '') = coalesce (s.genreKey, '')
# MAGIC     and coalesce(t.platformKey, '') = coalesce (s.platformKey, '')
# MAGIC     and coalesce(t.themeKey, '') = coalesce (s.themeKey, '')
# MAGIC when matched and (coalesce(t.hash, '') != coalesce(s.hash, '')) then
# MAGIC     update set *
# MAGIC when not matched then
# MAGIC     insert *


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
