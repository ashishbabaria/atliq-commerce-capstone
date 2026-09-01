-- ============================================================================
--  AtliQ Commerce  |  Pipeline Audit / Run Log  (Reliability - M7)
--  A tiny audit table the nightly pipeline writes to at the start and end of
--  every run. This is your "black box recorder": if a dashboard looks wrong,
--  you check here first - what ran, when, how many rows, pass or fail.
-- ============================================================================

IF SCHEMA_ID('etl') IS NULL EXEC('CREATE SCHEMA etl;');
GO

IF OBJECT_ID('etl.run_log', 'U') IS NOT NULL DROP TABLE etl.run_log;
GO

CREATE TABLE etl.run_log (
    run_id        BIGINT IDENTITY(1,1) NOT NULL,
    run_start_at  DATETIME2(0)  NOT NULL,
    run_end_at    DATETIME2(0)  NULL,
    stage         NVARCHAR(30)  NOT NULL,   -- 'ingest' | 'silver' | 'gold'
    object_name   NVARCHAR(128) NULL,       -- table / model, NULL for stage-level rows
    rows_affected BIGINT        NULL,
    status        NVARCHAR(20)  NOT NULL,   -- 'started' | 'success' | 'failed'
    message       NVARCHAR(400) NULL,
    CONSTRAINT PK_run_log PRIMARY KEY (run_id)
);
GO

-- Called at the start of a stage
IF OBJECT_ID('etl.usp_log_start', 'P') IS NOT NULL DROP PROCEDURE etl.usp_log_start;
GO
CREATE PROCEDURE etl.usp_log_start
    @run_start_at DATETIME2(0),
    @stage        NVARCHAR(30),
    @object_name  NVARCHAR(128) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    INSERT INTO etl.run_log (run_start_at, stage, object_name, status)
    VALUES (@run_start_at, @stage, @object_name, 'started');
END;
GO

-- Called after a stage/object finishes
IF OBJECT_ID('etl.usp_log_finish', 'P') IS NOT NULL DROP PROCEDURE etl.usp_log_finish;
GO
CREATE PROCEDURE etl.usp_log_finish
    @run_start_at  DATETIME2(0),
    @stage         NVARCHAR(30),
    @object_name   NVARCHAR(128) = NULL,
    @rows_affected BIGINT        = NULL,
    @status        NVARCHAR(20)  = 'success',
    @message       NVARCHAR(400) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    UPDATE etl.run_log
       SET run_end_at = SYSUTCDATETIME(),
           rows_affected = @rows_affected,
           status = @status,
           message = @message
     WHERE run_start_at = @run_start_at
       AND stage = @stage
       AND (object_name = @object_name OR (@object_name IS NULL AND object_name IS NULL))
       AND status = 'started';
END;
GO

PRINT 'ETL run_log audit table + logging procs created.';
GO
