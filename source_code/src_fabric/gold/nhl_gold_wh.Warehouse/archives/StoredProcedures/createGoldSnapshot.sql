CREATE   PROCEDURE archives.createGoldSnapshot
    @CreatedTables NVARCHAR(MAX) OUTPUT  -- This sends data back to the pipeline
AS
BEGIN
    DECLARE @Timestamp NVARCHAR(20) = FORMAT(GETDATE(), 'yyyyMMdd_HHmmss');
    DECLARE @SQL NVARCHAR(MAX);
    
    -- Define the new names
    DECLARE @Name1 NVARCHAR(128) = 'fact_skater_stats_per_60_' + @Timestamp;
    DECLARE @Name2 NVARCHAR(128) = 'fact_goalie_stats_per_60_' + @Timestamp;

    -- 1. Snapshot Skater Stats
    SET @SQL = N'CREATE TABLE archives.' + QUOTENAME(@Name1) + 
               N' AS SELECT * FROM gold.fact_skater_stats_per_60';
    EXEC sp_executesql @SQL;

    -- 2. Snapshot Goalie Stats
    SET @SQL = N'CREATE TABLE archives.' + QUOTENAME(@Name2) + 
               N' AS SELECT * FROM gold.fact_goalie_stats_per_60';
    EXEC sp_executesql @SQL;

    -- 3. Pass the names back as a comma-separated string
    SET @CreatedTables = @Name1 + ',' + @Name2;
END