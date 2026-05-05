-- Auto Generated (Do not modify) 8689B6EF3747798D9DE407BF1E716B79B8FC8BA508A4FE74D2CE5683E510989B


create view dev.loadReviewStats as
with cteExecutions as (
    select
        run_id
        , execution_start_time
        , execution_end_time
        , retrieved_reviews
        , app_id
        , execution_status
        -- 1. Calculate duration for EACH individual execution
        , datediff(minute, execution_start_time, execution_end_time) as exec_duration

        -- 2. Window functions to calculate percentiles across all executions partitioned by run_id
        , min(datediff(minute, execution_start_time, execution_end_time)) over (partition by run_id) as min_duration
        , percentile_cont(0.25) within group (
            order by datediff(minute, execution_start_time, execution_end_time)
        ) over (partition by run_id) as [25th_duration]
        , percentile_cont(0.75) within group (
            order by datediff(minute, execution_start_time, execution_end_time)
        ) over (partition by run_id) as [75th_duration]
        , max(datediff(minute, execution_start_time, execution_end_time)) over (partition by run_id) as max_duration
    from dev.loadControlReviews
)
select
    run_id
    -- Run-level start/end and total duration
    , min(execution_start_time) as run_start_time
    , max(execution_end_time) as run_end_time
    , datediff(minute, min(execution_start_time), max(execution_end_time)) as run_duration

    -- Execution duration stats (aggregated from the CTE)
    -- We use MAX() here because the window function in the CTE duplicates the exact same percentile value for all rows in a run_id
    , max(min_duration) as min_duration
    , max([25th_duration]) as [25th_duration]
    , cast(avg(cast(exec_duration as decimal(10, 2))) as decimal(10, 2)) as avg_duration
    , max([75th_duration]) as [75th_duration]
    , max(max_duration) as max_duration

    -- Run-level volume metrics
    , sum(retrieved_reviews) as retrieved_reviews
    , count(distinct app_id) as processed_games
    , sum(case when execution_status = 'empty' then 1 else 0 end) as empty_games
    , sum(case when execution_status not in ('success', 'empty', 'in-progress') then 1 else 0 end) as failed_executions
from cteExecutions
group by
    run_id
