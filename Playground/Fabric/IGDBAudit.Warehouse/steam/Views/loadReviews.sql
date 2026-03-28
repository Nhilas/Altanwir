-- Auto Generated (Do not modify) 48423D9ABB762ED62DAF509BE5D7EDB0F04FA3FBE4EF4A1D3C1B012A2C78FA05


create view steam.loadReviews as 
with ordered_executions as (
        select
            *
            , ROW_NUMBER() over ( PARTITION by app_id, execution_type order by execution_start_time desc ) as rn
        from steam.loadControlReviews 
    )

, successful_executions as (
        select
            app_id
            , execution_type
            , execution_id
            , first_retrieved_timestamp
            , last_retrieved_cursor
            , ROW_NUMBER() over ( PARTITION by app_id, execution_type order by execution_start_time desc ) as rn
        from steam.loadControlReviews
        where execution_status = 'success'
    )

select
    o.app_id
    , o.load_type
    , o.priority
    , case 
        when coalesce(e.execution_id, s.execution_id) is not null and o.load_status <> 'completed' then 0   -- prioritize ongoing runs
        when o.priority = 'High' then 1
        when o.priority = 'Medium' then 2
        when o.priority = 'Low' then 3
    end as priority_order
    , o.load_status
    , e.run_id
    , e.execution_id
    , e.execution_start_time
    , e.execution_end_time
    , e.execution_duration
    , e.execution_status
    , e.retrieved_reviews
    , cast(dateadd(second, e.first_retrieved_timestamp, '1970-01-01') as datetime2) as first_review_on
    , cast(dateadd(second, e.last_retrieved_timestamp, '1970-01-01') as datetime2) as last_review_on
    , e.output_path
    , s.first_retrieved_timestamp as high_water_mark
    , coalesce(e.last_retrieved_cursor, s.last_retrieved_cursor) as last_retrieved_cursor    -- if there is no cursor in the last run, see if the last successful run has one
from steam.loadOrchestratorReviews as o
left join ordered_executions as e
    on o.app_id = e.app_id
    and o.load_type = e.execution_type
    and e.rn = 1
left join successful_executions as s
    on o.app_id = s.app_id
    and o.load_type = s.execution_type
    and s.rn = 1