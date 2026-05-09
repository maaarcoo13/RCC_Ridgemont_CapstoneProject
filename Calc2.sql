-- zvOHAllocationCalc2 -- 
USE RCC_Datastore;
GO

/* 1. Create landing raw table from source if it does not exist */
IF OBJECT_ID('landing.zvOHAllocationCalc2_raw', 'U') IS NULL
BEGIN
    SELECT TOP (0) *
    INTO landing.zvOHAllocationCalc2_raw
    FROM RCC_Source.dbo.zvOHAllocationCalc2;
END
GO

/* 2. Add metadata columns for historical raw tracking */
IF COL_LENGTH('landing.zvOHAllocationCalc2_raw', 'LoadDate') IS NULL
BEGIN
    ALTER TABLE landing.zvOHAllocationCalc2_raw
    ADD LoadDate DATETIME NULL;
END
GO

IF COL_LENGTH('landing.zvOHAllocationCalc2_raw', 'RunID') IS NULL
BEGIN
    ALTER TABLE landing.zvOHAllocationCalc2_raw
    ADD RunID INT NULL;
END
GO

/* 3. Create staging table if it does not exist */
IF OBJECT_ID('stg.zvOHAllocationCalc2', 'U') IS NULL
BEGIN
    SELECT TOP (0) *
    INTO stg.zvOHAllocationCalc2
    FROM RCC_Source.dbo.zvOHAllocationCalc2;
END
GO

/* 4. Create curated table if it does not exist */
IF OBJECT_ID('dbo.zvOHAllocationCalc2', 'U') IS NULL
BEGIN
    SELECT TOP (0) *
    INTO dbo.zvOHAllocationCalc2
    FROM RCC_Source.dbo.zvOHAllocationCalc2;
END
GO

/*==============================================
=============== Pipeline starts ================*/

USE RCC_Datastore;
GO

CREATE OR ALTER PROCEDURE dbo.sp_Run_zvOHAllocationCalc2_Pipeline
AS 
BEGIN 
    SET NOCOUNT ON;

    DECLARE @RunID VARCHAR(100) = 'RUN_' + FORMAT(GETDATE(), 'yyyyMMdd_HHmmss');
    DECLARE @Step NVARCHAR(100) = 'Start';
    DECLARE @StepStart DATETIME; 
    DECLARE @InsertedCount INT = 0;
    DECLARE @UpdatedCount INT = 0;
    DECLARE @SourceRowCount INT = 0;
    DECLARE @LandingRowCount INT = 0;
    DECLARE @StagingRowCount INT = 0; 
   

    DECLARE @MergeResults TABLE
    (
        MergeAction NVARCHAR(10)
    );


    BEGIN TRY

        /* Count source rows */
        SET @Step = 'Count source rows';

        SELECT @SourceRowCount = COUNT(*)
        FROM RCC_Source.dbo.zvOHAllocationCalc2;

        /* Load Landing */
        SET @Step = 'Landing';
        SET @StepStart = GETDATE(); 

        TRUNCATE TABLE landing.zvOHAllocationCalc2_raw; 

        INSERT INTO landing.zvOHAllocationCalc2_raw
        (
            [Job],
            [JCDept],
            [BillYear],
            [BillMonth],
            [Date],
            [TotalHours],
            [BilledAmt],
            [Retainage],
            [BilledOwed],
            [LoadDate],
            [RunID]
        )
        SELECT
            [Job],
            [JCDept],
            [BillYear],
            [BillMonth],
            [Date],
            [TotalHours],
            [BilledAmt],
            [Retainage],
            [BilledOwed],
            GETDATE(),
            @RunID
        FROM RCC_Source.dbo.zvOHAllocationCalc2;

        SELECT @LandingRowCount = COUNT(*) FROM landing.zvOHAllocationCalc2_raw;

        /* Log lanidng layer */
        INSERT INTO dbo.SyncRunLog
        (
            RunID,
            DatasetName,
            Layer,
            ActionType,
            SourceRowCount,
            InsertedCount,
            UpdatedCount,
            Status,
            ErrorMessage,
            StartTime,
            EndTime,
            DataFreshness
        )
        VALUES
        (
            @RunID,
            'zvOHAllocationCalc2',
            'Landing',
            'Full Load',
            @SourceRowCount,
            @LandingRowCount,
            0,
            'Success',              
            NULL,                   
            @StepStart,             
            GETDATE(),             
            NULL 

        );

        /* Refresh staging */
        SET @Step = 'Staging';
        SET @StepStart = GETDATE();

        TRUNCATE TABLE stg.zvOHAllocationCalc2;

        INSERT INTO stg.zvOHAllocationCalc2
        (
            [Job],
            [JCDept],
            [BillYear],
            [BillMonth],
            [Date],
            [TotalHours],
            [BilledAmt],
            [Retainage],
            [BilledOwed]
        )
        SELECT DISTINCT
            [Job],
            [JCDept],
            [BillYear],
            [BillMonth],
            [Date],
            [TotalHours],
            [BilledAmt],
            [Retainage],
            [BilledOwed]
        FROM landing.zvOHAllocationCalc2_raw
        WHERE RunID = @RunID
            AND [Job] IS NOT Null
            AND [JCDept] IS NOT Null
            AND [BillYear] IS NOT Null
            AND [BillMonth] IS NOT Null;

        SELECT @StagingRowCount = COUNT(*) FROM stg.zvOHAllocationCalc2; 

      /* Log staging layer */

        INSERT INTO dbo.SyncRunLog
        (
            RunID,
            DatasetName,
            Layer,
            ActionType,
            SourceRowCount,
            InsertedCount,
            UpdatedCount,
            Status,
            ErrorMessage,
            StartTime,
            EndTime,
            DataFreshness
        )
        VALUES
        (
            @RunID,
            'zvOHAllocationCalc2',  
            'Staging',              
            'Truncate & Load',            
            @SourceRowCount,        
            @StagingRowCount,       
            0,                      
            'Success',              
            NULL,                   
            @StepStart,             
            GETDATE(),              
            NULL              
        );
     
        /* Merge staging into local curataed */
        SET @Step = 'Merge curated';
        SET @StepStart = GETDATE();

        MERGE dbo.zvOHAllocationCalc2 AS target
        USING stg.zvOHAllocationCalc2 AS source
         ON target.[Job] = source.[Job]
        AND target.[JCDept] = source.[JCDept]
        AND target.[BillYear] = source.[BillYear]
        AND target.[BillMonth] = source.[BillMonth]
        AND target.[BilledAmt] = source.[BilledAmt]
        AND target.[Retainage] = source.[Retainage]

        WHEN MATCHED AND (
               ISNULL(target.[Date], '19000101') <> ISNULL(source.[Date], '19000101')
            OR ISNULL(target.[TotalHours], -1) <> ISNULL(source.[TotalHours], -1)
            OR ISNULL(target.[BilledOwed], -1) <> ISNULL(source.[BilledOwed], -1)
        )
        THEN
            UPDATE SET
                target.[Date] = source.[Date],
                target.[TotalHours] = source.[TotalHours],
                target.[BilledOwed] = source.[BilledOwed]

        WHEN NOT MATCHED BY TARGET THEN
            INSERT
            (
                [Job],
                [JCDept],
                [BillYear],
                [BillMonth],
                [Date],
                [TotalHours],
                [BilledAmt],
                [Retainage],
                [BilledOwed]
            )
            VALUES
            (
                source.[Job],
                source.[JCDept],
                source.[BillYear],
                source.[BillMonth],
                source.[Date],
                source.[TotalHours],
                source.[BilledAmt],
                source.[Retainage],
                source.[BilledOwed]
            )

       OUTPUT $action INTO @MergeResults(MergeAction);

       /* Count actual Merge actions */
       SET @Step = 'Count merge actions';

       SELECT @InsertedCount = COUNT(*)
       FROM @MergeResults
       WHERE MergeAction = 'INSERT';

       SELECT @UpdatedCount = COUNT(*)
       FROM @MergeResults
       WHERE MergeAction = 'UPDATE';

        /* Log curated layer */

        INSERT INTO dbo.SyncRunLog
        (
            RunID,
            DatasetName,
            Layer,
            ActionType,
            SourceRowCount,
            InsertedCount,
            UpdatedCount,
            Status,
            ErrorMessage,
            StartTime,
            EndTime,
            DataFreshness
        )
        
        VALUES
        (
            @RunID,
            'zvOHAllocationCalc2',  
            'Curated',              
            'Upsert',               
            @SourceRowCount,       
            @InsertedCount,       
            @UpdatedCount,         
            'Success',             
            NULL,                   
            @StepStart,             
            GETDATE(),              
            GETDATE()               
        );
    END TRY

    -- Inserts failure row instead of updating a single row
    BEGIN CATCH

        INSERT INTO dbo.SyncRunLog
        (
            RunID,
            DatasetName,
            Layer,
            ActionType,
            SourceRowCount,
            InsertedCount,
            UpdatedCount,
            Status,
            ErrorMessage,
            StartTime,
            EndTime,
            DataFreshness
        )
        VALUES
        (
            @RunID,
            'zvOHAllocationCalc2',
            @Step,
            'Error',
            @SourceRowCount,
            0,
            0,
            'Failed',
            @Step + ': ' + ERROR_MESSAGE(),
            GETDATE(),
            GETDATE(),
            GETDATE()
        );

        THROW;
    END CATCH;
END;
GO
        
/*
===================   End of the pipeline query  ===============
===============================================================*/
-- A stored function for the pipeleine, to make the sql server agent step becomes very simple( repeatable run is possible)
EXEC dbo.sp_Run_zvOHAllocationCalc2_Pipeline;


/* Issue about this dataset:
Exact duplicate rows are gone after DISTINCT function on stg */
SELECT
    Job, JCDept, BillYear, BillMonth, [Date],
    TotalHours, BilledAmt, Retainage, BilledOwed,
    COUNT(*) AS Cnt
FROM stg.zvOHAllocationCalc2
GROUP BY
    Job, JCDept, BillYear, BillMonth, [Date],
    TotalHours, BilledAmt, Retainage, BilledOwed
HAVING COUNT(*) > 1;

-- But still duplicate rows(?) are shown with the grain keys
SELECT
    Job, BillYear, BillMonth,
    COUNT(*) AS Cnt
FROM stg.zvOHAllocationCalc2
GROUP BY Job, BillYear, BillMonth
HAVING COUNT(*) > 1;

-- Detail look: duplicate about Job = '1700803'
SELECT *
FROM stg.zvOHAllocationCalc2
WHERE Job = '1700803-'
  AND BillYear = 2019
  AND BillMonth = 1;

 -- Including Retainage and BilledAmt make unique for keys >> technical key
SELECT
    Job, BillYear, BillMonth, BilledAmt,Retainage,
    COUNT(*) AS Cnt
FROM stg.zvOHAllocationCalc2
GROUP BY
    Job, BillYear, BillMonth, BilledAmt, Retainage
HAVING COUNT(*) > 1;





------------ Extra check the data types of the dataset
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    NUMERIC_PRECISION,
    NUMERIC_SCALE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'zvOHAllocationCalc2'
ORDER BY ORDINAL_POSITION;


-- Hygine check
SELECT TOP 20*
FROM dbo.SyncRunLog
ORDER BY RunID DESC;


-- Freshness View across the datasets
CREATE VIEW dbo.vw_DataFreshnessStatus AS
SELECT
    PipelineName,
    MAX(DataFreshnessTime) AS LatestDataFreshness,
    MAX(EndTime) AS LastRunTime,
    MAX(Status) AS LastStatus
FROM dbo.SyncRunLog
GROUP BY PipelineName;


DROP TABLE dbo.SyncRunLog;

CREATE TABLE dbo.SyncRunLog
(
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


SELECT RunID, DatasetName, Layer, ActionType, SourceRowCount, InsertedCount, UpdatedCount, Status, ErrorMessage, StartTime, EndTime, DataFreshness
FROM dbo.SyncRunLog
WHERE DatasetName = 'zvAPVMCo1'
AND RunID = (SELECT TOP 1 RunID FROM dbo.SyncRunLog WHERE DatasetName = 'zvAPVMCo1' ORDER BY StartTime DESC)

