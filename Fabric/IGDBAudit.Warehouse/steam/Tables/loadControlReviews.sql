CREATE TABLE [steam].[loadControlReviews] (

	[app_id] int NOT NULL, 
	[run_id] varchar(max) NULL, 
	[execution_id] varchar(max) NULL, 
	[execution_type] varchar(30) NULL, 
	[execution_start_time] datetime2(3) NULL, 
	[execution_end_time] datetime2(3) NULL, 
	[execution_duration] int NULL, 
	[execution_status] varchar(max) NULL, 
	[retrieved_reviews] int NULL, 
	[first_retrieved_timestamp] bigint NULL, 
	[last_retrieved_timestamp] bigint NULL, 
	[last_retrieved_cursor] varchar(30) NULL, 
	[output_path] varchar(max) NULL, 
	[is_loaded] bit NULL
);