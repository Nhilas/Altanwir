CREATE TABLE [dev].[loadOrchestratorReviews] (

	[app_id] int NOT NULL, 
	[load_type] varchar(30) NULL, 
	[priority] varchar(30) NULL, 
	[load_status] varchar(max) NULL
);