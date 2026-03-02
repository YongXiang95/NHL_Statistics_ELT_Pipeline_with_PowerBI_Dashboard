CREATE OR ALTER PROCEDURE admin.usp_publish_gold_per_60
    @pipeline_id       VARCHAR(100),
    @pipeline_run_id   VARCHAR(100),
    @stg_skater_table  VARCHAR(200),
    @stg_goalie_table  VARCHAR(200)
AS
BEGIN
    SET NOCOUNT ON;

    -- Timestamp for snapshots and audit
    DECLARE @publish_ts DATETIME2(3) = SYSUTCDATETIME();
    DECLARE @ts_suffix  VARCHAR(30) = FORMAT(@publish_ts, 'yyyyMMdd_HHmmss');

    -- Fixed gold table names
    DECLARE @gold_skater_table VARCHAR(200) = 'gold.fact_skater_stats_per_60';
    DECLARE @gold_goalie_table VARCHAR(200) = 'gold.fact_goalie_stats_per_60';

    -- Snapshot table names
    DECLARE @snap_skater_table VARCHAR(300) = 'archives.fact_skater_stats_per_60_' + @ts_suffix;
    DECLARE @snap_goalie_table VARCHAR(300) = 'archives.fact_goalie_stats_per_60_' + @ts_suffix;

    -- Temp table for pipeline output
    CREATE TABLE #results (
        gold_table     VARCHAR(200),
        staging_table  VARCHAR(300),
        snapshot_table VARCHAR(300),
        publish_ts     DATETIME2(3)
    );

    BEGIN TRY
        BEGIN TRAN;

        -- SKATER
        EXEC('CREATE TABLE ' + @snap_skater_table + ' AS SELECT * FROM ' + @gold_skater_table);
        EXEC('TRUNCATE TABLE ' + @gold_skater_table);
        EXEC('INSERT INTO ' + @gold_skater_table + ' SELECT * FROM ' + @stg_skater_table);

        INSERT INTO admin.gold_publish_audit
        VALUES (@pipeline_id, @pipeline_run_id, @publish_ts, @gold_skater_table, @stg_skater_table, @snap_skater_table);

        INSERT INTO #results
        VALUES (@gold_skater_table, @stg_skater_table, @snap_skater_table, @publish_ts);

        -- GOALIE
        EXEC('CREATE TABLE ' + @snap_goalie_table + ' AS SELECT * FROM ' + @gold_goalie_table);
        EXEC('TRUNCATE TABLE ' + @gold_goalie_table);
        EXEC('INSERT INTO ' + @gold_goalie_table + ' SELECT * FROM ' + @stg_goalie_table);

        INSERT INTO admin.gold_publish_audit
        VALUES (@pipeline_id, @pipeline_run_id, @publish_ts, @gold_goalie_table, @stg_goalie_table, @snap_goalie_table);

        INSERT INTO #results
        VALUES (@gold_goalie_table, @stg_goalie_table, @snap_goalie_table, @publish_ts);

        COMMIT;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK;
        THROW;
    END CATCH;

    -- Return results to Fabric pipeline
    SELECT gold_table, staging_table, snapshot_table, publish_ts
    FROM #results;
END;