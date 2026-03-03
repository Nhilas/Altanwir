{% set game_columns = range(38) %}

with source_data as (
    {% if target=='prod' %}
        select
        {% for i in game_columns %}
            _c{{ i }} as column{{ '%02d' | format(i) }}{% if not loop.last %},{% endif %}
        {% endfor %}
        from {{ source('fabricSteam', 'games') }}
    {% else %}
        select * from {{ source('localSteam', 'games') }}
    {% endif %}
)

, renamed as (
    select
        column00 as gameId
        , column01 as gameName
        , cast(try_strptime(column02, '%b %d, %Y') as date) as releaseDate
        , column03 as estimatedOwners
        , column04 as peakCCU
        , column05 as requiredAge
        , column06 as price
        , column09 as aboutTheGame
        , column10 as supportedLanguages
        , column11 as fullAudioLanguages
        , column14 as website
        , column17 as onWindows
        , column18 as onMac
        , column19 as onLinux
        , column26 as achievements
        , column27 as recommendations
        , column28 as notes
        , column29 as averagePlaytimeForever
        , column31 as medianPlaytimeForever
        , column33 as developers
        , column34 as publishers
        , column35 as categories
        , column36 as genres
        , column37 as tags
    from source_data
    {% if var("release_date", none) is not none %}
        where cast(try_strptime(column02, '%b %d, %Y') as date) >= cast('{{ var("release_date") }}' as date)
    {% endif %}            
)

, splitted as (
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
        --, string_split(supportedLanguages,',') as supportedLanguages
        --, string_split(fullAudioLanguages, ',') as fullAudioLanguages
        , website
        , string_split(
            CONCAT_WS(',',        -- CONCAT_WS ignores NULL values, so only the platforms that are 'True' will be included in the resulting string
                CASE WHEN onWindows = 'True' THEN 'Windows' END,
                CASE WHEN onMac = 'True' THEN 'Mac' END,
                CASE WHEN onLinux = 'True' THEN 'Linux' END
            ), ','
        ) as platform
        , achievements
        , recommendations
        , notes
        , averagePlaytimeForever
        , medianPlaytimeForever
        , string_split(developers,',') as developers
        , string_split(publishers,',') as publishers
        , string_split(categories,',') as categories
        , string_split(genres,',') as genres
        , string_split(tags,',') as tags
    from renamed
)

select *
from splitted