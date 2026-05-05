CREATE TABLE [dev].[versionControl] (

	[table_name] varchar(50) NULL, 
	[run_id] varchar(50) NULL, 
	[change_type] varchar(30) NULL, 
	[commit_version] bigint NULL, 
	[commit_timestamp] datetime2(3) NULL, 
	[rows_inserted] bigint NULL, 
	[rows_updated] bigint NULL, 
	[latest_source_version] bigint NULL, 
	[audit_notes] varchar(max) NULL
);