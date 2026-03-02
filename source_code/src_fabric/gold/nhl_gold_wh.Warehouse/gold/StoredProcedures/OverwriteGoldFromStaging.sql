CREATE   PROCEDURE gold.OverwriteGoldFromStaging
    @skater NVARCHAR(MAX), -- This matches the 'skater' parameter from your pipeline
    @goalie NVARCHAR(MAX), -- This matches the 'goalie' parameter from your pipeline
    @StatusMessage NVARCHAR(MAX) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @SQL NVARCHAR(MAX);

    -- 1. Overwrite Gold Skater Stats
    IF EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id 
               WHERE s.name = 'gold' AND t.name = 'fact_skater_stats_per_60')
    BEGIN
        DROP TABLE gold.fact_skater_stats_per_60;
    END

    -- Use the @skater variable here
    SET @SQL = N'CREATE TABLE gold.fact_skater_stats_per_60 AS SELECT * FROM ' + @skater;
    EXEC sp_executesql @SQL;


    -- 2. Overwrite Gold Goalie Stats
    IF EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id 
               WHERE s.name = 'gold' AND t.name = 'fact_goalie_stats_per_60')
    BEGIN
        DROP TABLE gold.fact_goalie_stats_per_60;
    END

    -- Use the @goalie variable here
    SET @SQL = N'CREATE TABLE gold.fact_goalie_stats_per_60 AS SELECT * FROM ' + @goalie;
    EXEC sp_executesql @SQL;

    -- 3. Pass back status
    SET @StatusMessage = 'Success';
END