-- Auto Generated (Do not modify) F13662BB98EFD3ED48BC0491E70BFF14F4932AA67D88C53941B00B165DB51EAD


create view dev.loadReviews as
with ordered_executions as (
        select
            *
            , ROW_NUMBER() over ( PARTITION by app_id, execution_type order by execution_start_time desc ) as rn
        from dev.loadControlReviews 
    )

, successful_executions as (
        select
            app_id
            , execution_type
            , execution_id
            , first_retrieved_timestamp
            , last_retrieved_cursor
            , ROW_NUMBER() over ( PARTITION by app_id, execution_type order by execution_start_time desc ) as rn
        from dev.loadControlReviews
        where execution_status = 'success'
    )

, top_water_marks as (
        select 
            app_id
            , max(first_retrieved_timestamp) as top_retrieved_timestamp
        from dev.loadControlReviews
        where execution_status = 'success'
        group by app_id
    )    

select
    o.app_id
    , o.load_type
    , o.priority
    , case 
        when o.load_status = 'retry' then -1 -- always prioritize reruns
        when coalesce(e.execution_id, s.execution_id) is not null and o.load_status <> 'completed' then 0   -- prioritize ongoing runs
        when o.load_status = 'completed' then 99
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
    , twm.top_retrieved_timestamp as high_water_mark
    , cast(dateadd(second, twm.top_retrieved_timestamp, '1970-01-01') as datetime2) as high_water_mark_date
    , case                                              -- expected behaviour: use the cursor from the last successful execution if the load is marked as failed or empty but the last saved cursor is '*'
        when o.load_status in ('empty', 'failed', 'retry') and (e.last_retrieved_cursor is null or e.last_retrieved_cursor = '*' ) and s.last_retrieved_cursor is not null
            then s.last_retrieved_cursor
        when o.load_status in ('completed', 'pending')  -- expected behaviour: if the load is completed or it hasn't started yet, make sure the cursor used will be '*'
            then '*'
        else coalesce(e.last_retrieved_cursor, '*')     -- if there is no valid cursor from either the last run, or the last succssful run, use '*'
    end as last_retrieved_cursor
from dev.loadOrchestratorReviews as o
left join top_water_marks as twm
    on o.app_id = twm.app_id  
left join ordered_executions as e
    on o.app_id = e.app_id
    and o.load_type = e.execution_type
    and e.rn = 1
left join successful_executions as s
    on o.app_id = s.app_id
    and o.load_type = s.execution_type
    and s.rn = 1