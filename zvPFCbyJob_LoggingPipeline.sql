--logging table w/ log ID

USE Ridgemont_Syncstore;


IF EXISTS (SELECT * FROM sys.tables t JOIN sys.schemas s ON t.schema_id = s.schema_id WHERE t.name = 'SyncRunLog' AND s.name = 'ops')
    DROP TABLE ops.SyncRunLog;
GO

CREATE TABLE ops.SyncRunLog (
    LogID          INT IDENTITY(1,1) PRIMARY KEY,
    RunID          VARCHAR(100)  NULL,
    DatasetName    VARCHAR(100)  NULL,
    Layer          VARCHAR(50)   NULL,
    ActionType     VARCHAR(50)   NULL,
    SourceRowCount INT           NULL,
    InsertedCount  INT DEFAULT 0 NULL,
    UpdatedCount   INT DEFAULT 0 NULL,
    Status         VARCHAR(50)   NULL,
    ErrorMessage   VARCHAR(MAX)  NULL,
    StartTime      DATETIME      NULL,
    EndTime        DATETIME      NULL,
    DataFreshness  DATETIME      NULL
);
GO

DROP PROCEDURE IF EXISTS ops.Run_Job_Pipeline;
GO

CREATE PROCEDURE ops.Run_Job_Pipeline
AS
BEGIN

    -- Generate one RunID shared across all 3 layer logs
    DECLARE @RunID VARCHAR(100) = 'RUN_' + FORMAT(SYSDATETIME(), 'yyyyMMdd_HHmm');

    DECLARE
        @StepStart      DATETIME = SYSDATETIME(),
        @StepEnd        DATETIME,
        @StepStatus     VARCHAR(50),
        @StepError      VARCHAR(MAX),
        @OverallStatus  VARCHAR(50) = 'Success',

        -- row counts
        @LandingCount   INT = 0,
        @StagingCount   INT = 0,
        @InsertedCount  INT = 0,
        @UpdatedCount   INT = 0,

        @FreshnessTime  DATETIME;

    -- capture merge actions
    DECLARE @MergeResults TABLE (ActionType NVARCHAR(10));

    --------------------------------------------------
    -- STEP 1: LOAD LANDING
    --------------------------------------------------
    SET @StepStart  = SYSDATETIME();
    SET @StepStatus = 'Success';
    SET @StepError  = NULL;

    BEGIN TRY
        TRUNCATE TABLE landing.zvPFCbyJob_raw;

        INSERT INTO landing.zvPFCbyJob_raw
        SELECT * FROM Ridgemont_Source.dbo.zvPFCbyJob;

        SELECT @LandingCount = COUNT(*) FROM landing.zvPFCbyJob_raw;

    END TRY
    BEGIN CATCH
        SET @StepStatus  = 'Failed';
        SET @StepError   = ERROR_MESSAGE();
        SET @OverallStatus = 'Failed';
    END CATCH;

    SET @StepEnd = SYSDATETIME();

    -- Log Landing
    INSERT INTO ops.SyncRunLog (
        RunID, DatasetName, Layer, ActionType,
        SourceRowCount, InsertedCount, UpdatedCount,
        Status, ErrorMessage, StartTime, EndTime, DataFreshness
    )
    VALUES (
        @RunID, 'zvPFCbyJob', 'Landing', 'Full Load',
        @LandingCount, @LandingCount, 0,
        @StepStatus, @StepError, @StepStart, @StepEnd, NULL
    );

    IF @OverallStatus = 'Failed' GOTO LogEnd;

    --------------------------------------------------
    -- STEP 2: LOAD STAGING (WITH CLEANING)
    --------------------------------------------------
    SET @StepStart  = SYSDATETIME();
    SET @StepStatus = 'Success';
    SET @StepError  = NULL;

    BEGIN TRY
        TRUNCATE TABLE staging.zvPFCbyJob;

        INSERT INTO staging.zvPFCbyJob
        SELECT 
            JCCo,
            Contract,
            Department,
            REPLACE(REPLACE(ISNULL(Description,''), CHAR(13),''), CHAR(10),'') AS Description,
            REPLACE(REPLACE(ISNULL(udCommonCust,''), CHAR(13),''), CHAR(10),'') AS udCommonCust,
            ISNULL(RevOrigAmt,0) AS RevOrigAmt,
            ISNULL(RevCurAmt,0) AS RevCurAmt,
            ISNULL(COAmt,0) AS COAmt,
            ISNULL(CostActualAmt,0) AS CostActualAmt,
            ISNULL(CostOrigAmt,0) AS CostOrigAmt,
            ISNULL(CostCurAmt,0) AS CostCurAmt,
            ISNULL(CostProjected,0) AS CostProjected,
            ISNULL(ProfitOrigAmt,0) AS ProfitOrigAmt,
            ISNULL(ProfitCurAmt,0) AS ProfitCurAmt,
            ISNULL(ProfitActual,0) AS ProfitActual,
            ISNULL(ActualGP,0) AS ActualGP,
            ISNULL(ProfitProj,0) AS ProfitProj,
            ISNULL(ProjectedGP,0) AS ProjectedGP,
            ISNULL(BilledAmt,0) AS BilledAmt,
            StartMonth,
            MonthClosed,
            ProjCloseDate,
            JobStatus,
            Year,
            Expr1,
            REPLACE(REPLACE(ISNULL(MailAddress,''), CHAR(13),''), CHAR(10),'') AS MailAddress,
            REPLACE(REPLACE(ISNULL(MailCity,''), CHAR(13),''), CHAR(10),'') AS MailCity,
            REPLACE(REPLACE(ISNULL(MailState,''), CHAR(13),''), CHAR(10),'') AS MailState,
            MailZip,
            REPLACE(REPLACE(ISNULL(MailAddress2,''), CHAR(13),''), CHAR(10),'') AS MailAddress2,
            REPLACE(REPLACE(ISNULL(ShipAddress,''), CHAR(13),''), CHAR(10),'') AS ShipAddress,
            REPLACE(REPLACE(ISNULL(ShipCity,''), CHAR(13),''), CHAR(10),'') AS ShipCity,
            REPLACE(REPLACE(ISNULL(ShipState,''), CHAR(13),''), CHAR(10),'') AS ShipState,
            ShipZip,
            REPLACE(REPLACE(ISNULL(ShipAddress2,''), CHAR(13),''), CHAR(10),'') AS ShipAddress2,
            REPLACE(REPLACE(ISNULL(udDirector,''), CHAR(13),''), CHAR(10),'') AS udDirector,
            ISNULL(TeamOrigin,0) AS TeamOrigin,
            ISNULL(TeamProj,0) AS TeamProj,
            ISNULL(TeamActual,0) AS TeamActual,
            ISNULL(ProfitEffOrig,0) AS ProfitEffOrig,
            ISNULL(ProfitEffProj,0) AS ProfitEffProj,
            ISNULL(ProfitEffActual,0) AS ProfitEffActual
        FROM landing.zvPFCbyJob_raw
        WHERE Contract IS NOT NULL;

        SELECT @StagingCount = COUNT(*) FROM staging.zvPFCbyJob;

    END TRY
    BEGIN CATCH
        SET @StepStatus  = 'Failed';
        SET @StepError   = ERROR_MESSAGE();
        SET @OverallStatus = 'Failed';
    END CATCH;

    SET @StepEnd = SYSDATETIME();

    -- Log Staging
    INSERT INTO ops.SyncRunLog (
        RunID, DatasetName, Layer, ActionType,
        SourceRowCount, InsertedCount, UpdatedCount,
        Status, ErrorMessage, StartTime, EndTime, DataFreshness
    )
    VALUES (
        @RunID, 'zvPFCbyJob', 'Staging', 'Truncate and Load',
        @LandingCount, @StagingCount, 0,
        @StepStatus, @StepError, @StepStart, @StepEnd, NULL
    );

    IF @OverallStatus = 'Failed' GOTO LogEnd;

    --------------------------------------------------
    -- STEP 3: MERGE INTO CURATED (INCREMENTAL)
    --------------------------------------------------
    SET @StepStart  = SYSDATETIME();
    SET @StepStatus = 'Success';
    SET @StepEnd    = NULL;
    SET @StepError  = NULL;

    BEGIN TRY

        MERGE curated.zvPFCbyJob AS target
        USING staging.zvPFCbyJob AS source
        ON target.[Contract] = source.[Contract]

        WHEN MATCHED AND (
            ISNULL(target.RevCurAmt,0)      <> ISNULL(source.RevCurAmt,0)      OR
            ISNULL(target.CostActualAmt,0)  <> ISNULL(source.CostActualAmt,0)  OR
            ISNULL(target.ProfitActual,0)   <> ISNULL(source.ProfitActual,0)   OR
            ISNULL(target.BilledAmt,0)      <> ISNULL(source.BilledAmt,0)      OR
            ISNULL(target.RevOrigAmt,0)     <> ISNULL(source.RevOrigAmt,0)     OR
            ISNULL(target.COAmt,0)          <> ISNULL(source.COAmt,0)          OR
            ISNULL(target.Description,'')   <> ISNULL(source.Description,'')   OR
            ISNULL(target.udDirector,'')    <> ISNULL(source.udDirector,'')
        )
        THEN UPDATE SET
            target.RevOrigAmt       = source.RevOrigAmt,
            target.RevCurAmt        = source.RevCurAmt,
            target.COAmt            = source.COAmt,
            target.CostActualAmt    = source.CostActualAmt,
            target.CostOrigAmt      = source.CostOrigAmt,
            target.CostCurAmt       = source.CostCurAmt,
            target.CostProjected    = source.CostProjected,
            target.ProfitOrigAmt    = source.ProfitOrigAmt,
            target.ProfitCurAmt     = source.ProfitCurAmt,
            target.ProfitActual     = source.ProfitActual,
            target.ActualGP         = source.ActualGP,
            target.ProfitProj       = source.ProfitProj,
            target.ProjectedGP      = source.ProjectedGP,
            target.BilledAmt        = source.BilledAmt,
            target.MonthClosed      = source.MonthClosed,
            target.ProjCloseDate    = source.ProjCloseDate,
            target.JobStatus        = source.JobStatus,
            target.Description      = source.Description,
            target.udCommonCust     = source.udCommonCust,
            target.udDirector       = source.udDirector,
            target.TeamOrigin       = source.TeamOrigin,
            target.TeamProj         = source.TeamProj,
            target.TeamActual       = source.TeamActual,
            target.ProfitEffOrig    = source.ProfitEffOrig,
            target.ProfitEffProj    = source.ProfitEffProj,
            target.ProfitEffActual  = source.ProfitEffActual,
            target.LastModifiedDate = SYSDATETIME()

        WHEN NOT MATCHED THEN
            INSERT (
                JCCo, Contract, Department, Description, udCommonCust,
                RevOrigAmt, RevCurAmt, COAmt, CostActualAmt, CostOrigAmt,
                CostCurAmt, CostProjected, ProfitOrigAmt, ProfitCurAmt,
                ProfitActual, ActualGP, ProfitProj, ProjectedGP,
                BilledAmt, StartMonth, MonthClosed, ProjCloseDate,
                JobStatus, Year, Expr1, MailAddress, MailCity,
                MailState, MailZip, MailAddress2, ShipAddress,
                ShipCity, ShipState, ShipZip, ShipAddress2,
                udDirector, TeamOrigin, TeamProj, TeamActual,
                ProfitEffOrig, ProfitEffProj, ProfitEffActual,
                CreatedDate, LastModifiedDate
            )
            VALUES (
                source.JCCo, source.Contract, source.Department, source.Description, source.udCommonCust,
                source.RevOrigAmt, source.RevCurAmt, source.COAmt, source.CostActualAmt, source.CostOrigAmt,
                source.CostCurAmt, source.CostProjected, source.ProfitOrigAmt, source.ProfitCurAmt,
                source.ProfitActual, source.ActualGP, source.ProfitProj, source.ProjectedGP,
                source.BilledAmt, source.StartMonth, source.MonthClosed, source.ProjCloseDate,
                source.JobStatus, source.Year, source.Expr1, source.MailAddress, source.MailCity,
                source.MailState, source.MailZip, source.MailAddress2, source.ShipAddress,
                source.ShipCity, source.ShipState, source.ShipZip, source.ShipAddress2,
                source.udDirector, source.TeamOrigin, source.TeamProj, source.TeamActual,
                source.ProfitEffOrig, source.ProfitEffProj, source.ProfitEffActual,
                SYSDATETIME(), SYSDATETIME()
            )

        OUTPUT $action INTO @MergeResults;

        -- Counts
        SELECT
            @InsertedCount = ISNULL(SUM(CASE WHEN ActionType = 'INSERT' THEN 1 ELSE 0 END),0),
            @UpdatedCount  = ISNULL(SUM(CASE WHEN ActionType = 'UPDATE' THEN 1 ELSE 0 END),0)
        FROM @MergeResults;

        -- Freshness
        SELECT @FreshnessTime = CAST(MAX(LastModifiedDate) AS DATETIME)
        FROM curated.zvPFCbyJob;

    END TRY
    BEGIN CATCH
        SET @StepStatus  = 'Failed';
        SET @StepError   = ERROR_MESSAGE();
        SET @OverallStatus = 'Failed';
    END CATCH;

    SET @StepEnd = SYSDATETIME();

    -- Log Curated
    INSERT INTO ops.SyncRunLog (
        RunID, DatasetName, Layer, ActionType,
        SourceRowCount, InsertedCount, UpdatedCount,
        Status, ErrorMessage, StartTime, EndTime, DataFreshness
    )
    VALUES (
        @RunID, 'zvPFCbyJob', 'Curated', 'Upsert',
        @StagingCount, @InsertedCount, @UpdatedCount,
        @StepStatus, @StepError, @StepStart, @StepEnd, @FreshnessTime
    );

    --------------------------------------------------
    -- STEP 4: UPDATE FRESHNESS TABLE
    --------------------------------------------------
    LogEnd:

    IF @OverallStatus = 'Success'
    BEGIN
        MERGE ops.DataFreshness AS target
        USING (
            SELECT
                'Job Pipeline'          AS PipelineName,
                CAST(SYSDATETIME() AS DATETIME) AS LastSuccessfulRun,
                @FreshnessTime          AS LastDataUpdate,
                @OverallStatus          AS Status
        ) AS source
        ON target.PipelineName = source.PipelineName

        WHEN MATCHED THEN
            UPDATE SET
                LastSuccessfulRun = source.LastSuccessfulRun,
                LastDataUpdate    = source.LastDataUpdate,
                Status            = source.Status

        WHEN NOT MATCHED THEN
            INSERT (PipelineName, LastSuccessfulRun, LastDataUpdate, Status)
            VALUES (source.PipelineName, source.LastSuccessfulRun, source.LastDataUpdate, source.Status);
    END;

END;
GO

-- Execute pipeline
EXEC ops.;
Run_Job_Pipeline
SELECT * FROM ops.SyncRunLog ORDER BY LogID DESC;

SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'SyncRunLog';








