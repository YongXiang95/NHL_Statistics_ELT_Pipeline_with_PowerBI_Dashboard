DROP TABLE IF EXISTS admin.gold_publish_audit;

CREATE TABLE admin.gold_publish_audit (
    pipeline_id     VARCHAR(100),     -- pipeline identifier
    run_id          VARCHAR(100),     -- pipeline run id
    publish_ts      DATETIME2(3),     -- timestamp of publish
    gold_table      VARCHAR(200),     -- gold table that was replaced
    staging_table   VARCHAR(300),     -- staging table that replaced it
    snapshot_table  VARCHAR(300)      -- snapshot table created
);