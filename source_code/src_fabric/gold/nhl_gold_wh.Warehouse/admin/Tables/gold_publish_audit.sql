CREATE TABLE [admin].[gold_publish_audit] (

	[pipeline_id] varchar(100) NULL, 
	[run_id] varchar(100) NULL, 
	[publish_ts] datetime2(3) NULL, 
	[gold_table] varchar(200) NULL, 
	[staging_table] varchar(300) NULL, 
	[snapshot_table] varchar(300) NULL
);