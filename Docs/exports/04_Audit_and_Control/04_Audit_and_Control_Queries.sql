-- C01_versioncontrol

select *
from steam.versionControl
order by commit_timestamp desc


-- C02_loadcontrol

select top 20 *
from steam.loadControlReviews
where retrieved_reviews > 0
order by execution_start_time desc

-- C03_loadcontrol_summary

SELECT
    execution_type,
    case when execution_status like 'failed%' then 'failed' else execution_status end as execution_status,
    COUNT(*) AS executions,
    SUM(retrieved_reviews) AS total_reviews,
    AVG(execution_duration) AS avg_duration_seconds
FROM steam.loadControlReviews
GROUP BY execution_type, case when execution_status like 'failed%' then 'failed' else execution_status end
ORDER BY execution_type, execution_status;

-- C04_top_games_by_reviews

;with steamGames as (
    select
        g.gameName
        , eg.eId
    from IGDBAnalytics.silver.games as g
    inner join IGDBAnalytics.silver.externalgames as eg
        on g.gameKey = eg.gameKey
    where
        eg.egSourceName = 'Steam'
        and eg.eId not like '%,%'
)

select
    o.app_id
    , o.load_type
    , o.load_status
    , sg.gameName
    , o.priority
    , count(distinct(c.execution_id)) as executions
    , sum(c.execution_duration) as total_duration_seconds
    , sum(c.retrieved_reviews) as total_reviews
    , max(cast(dateadd(second, c.first_retrieved_timestamp, '1970-01-01') as datetime2)) as first_review_on
    , min(cast(dateadd(second, c.last_retrieved_timestamp, '1970-01-01') as datetime2)) as last_review_on
    , max(c.execution_start_time) as last_execution_on
from steam.loadOrchestratorReviews as o
inner join steam.loadControlReviews as c
    on c.app_id = o.app_id
    and c.execution_type = o.load_type
left join steamGames as sg
    on o.app_id = sg.eId
where 1=1
    -- and o.app_id = '1330235937'
group by
    o.app_id
    , o.load_type
    , sg.gameName
    , o.priority
    , o.load_status
order by
    total_reviews desc
