CREATE TABLE [bronze].[steamreviews] (

	[app_id] bigint NULL, 
	[recommendationid] varchar(8000) NULL, 
	[review_json] varchar(8000) NULL, 
	[insert_execution_id] varchar(8000) NULL, 
	[update_execution_id] varchar(8000) NULL, 
	[insert_run_id] varchar(8000) NULL, 
	[update_run_id] varchar(8000) NULL
);