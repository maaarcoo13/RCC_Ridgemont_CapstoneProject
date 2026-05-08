USE CapstoneProject;
GO

-------------------------------------------------------------------------------
-- 1. CREATE SCHEMA AND CURATED TABLE
-------------------------------------------------------------------------------
-- Create the schema if it does not exist
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'curated')
BEGIN
    EXEC('CREATE SCHEMA [curated]');
    PRINT 'SUCCESS: Schema [curated] created.';
END;
GO

-- Create the Curated table inside the new schema
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'PRJobRev' AND schema_id = SCHEMA_ID('curated'))
BEGIN
    CREATE TABLE curated.PRJobRev (
        JobID INT,
        JCDept INT,
        BillYear INT,
        BillMonth INT,
        [Date] DATETIME,
        TotalHours FLOAT,
        BilledAmt FLOAT,
        Retainage FLOAT,
        BilledOwed FLOAT,
        Description VARCHAR(255)
    );
    PRINT 'SUCCESS: Table curated.PRJobRev created.';
END
ELSE
BEGIN
    PRINT 'NOTICE: Table curated.PRJobRev already exists. Skipping creation.';
END;
GO

-------------------------------------------------------------------------------
-- 2. STORED PROCEDURE FOR CURATED LAYER (UPSERT LOGIC)
-------------------------------------------------------------------------------
CREATE OR ALTER PROCEDURE dbo.sp_LoadCurated_PRJobRev
    @RunID VARCHAR(100) = 'MANUAL_LOCAL_RUN'
AS
BEGIN
    SET NOCOUNT ON;

    -- Local variables for logging
    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @EndTime DATETIME;
    DECLARE @DataFreshnessTime DATETIME;
    DECLARE @SourceRows INT = 0;
    DECLARE @InsertedRows INT = 0;
    DECLARE @UpdatedRows INT = 0;
    DECLARE @ErrorMsg VARCHAR(MAX);

    DECLARE @MergeSummary TABLE (ActionTaken VARCHAR(20));

    BEGIN TRY
        -- A. Calculate Data Freshness and Total Source Rows
        SELECT @DataFreshnessTime = MAX(CAST([Date] AS DATETIME)) FROM dbo.stg_PRJobRev WHERE [Date] IS NOT NULL AND [Date] <> '';
        SELECT @SourceRows = COUNT(*) FROM dbo.stg_PRJobRev;

        -- B. Clean and Format data using a CTE
        WITH CleanedSource AS (
            SELECT 
                CAST(REPLACE(Job, '-', '') AS INT) AS JobID,
                CAST(REPLACE(JCDept, '-', '') AS INT) AS JCDept,
                CAST(BillYear AS INT) AS BillYear,
                CAST(BillMonth AS INT) AS BillMonth,
                CAST([Date] AS DATETIME) AS [Date],
                CAST(TotalHours AS FLOAT) AS TotalHours,
                CAST(NULLIF(BilledAmt, '') AS FLOAT) AS BilledAmt,
                CAST(NULLIF(Retainage, '') AS FLOAT) AS Retainage,
                CAST(NULLIF(BilledOwed, '') AS FLOAT) AS BilledOwed,
                Description
            FROM dbo.stg_PRJobRev
            WHERE Job IS NOT NULL AND Job <> ''
        )

        -- C. Execute the MERGE (Upsert) operation against curated.PRJobRev
        MERGE INTO curated.PRJobRev AS Target
        USING CleanedSource AS Source
        ON Target.JobID = Source.JobID 
           AND Target.JCDept = Source.JCDept 
           AND Target.[Date] = Source.[Date]

        WHEN MATCHED THEN
            UPDATE SET 
                Target.BillYear = Source.BillYear,
                Target.BillMonth = Source.BillMonth,
                Target.TotalHours = Source.TotalHours,
                Target.BilledAmt = Source.BilledAmt,
                Target.Retainage = Source.Retainage,
                Target.BilledOwed = Source.BilledOwed,
                Target.Description = Source.Description

        WHEN NOT MATCHED BY TARGET THEN
            INSERT (JobID, JCDept, BillYear, BillMonth, [Date], TotalHours, BilledAmt, Retainage, BilledOwed, Description)
            VALUES (Source.JobID, Source.JCDept, Source.BillYear, Source.BillMonth, Source.[Date], Source.TotalHours, Source.BilledAmt, Source.Retainage, Source.BilledOwed, Source.Description)
        
        OUTPUT $action INTO @MergeSummary;

        -- D. Finalize metrics
        SELECT @InsertedRows = COUNT(*) FROM @MergeSummary WHERE ActionTaken = 'INSERT';
        SELECT @UpdatedRows = COUNT(*) FROM @MergeSummary WHERE ActionTaken = 'UPDATE';
        SET @EndTime = GETDATE();

        -- E. Record successful execution in the new SyncRunLog table
        INSERT INTO dbo.SyncRunLog 
            (RunID, DatasetName, Layer, ActionType, SourceRowCount, InsertedCount, UpdatedCount, Status, ErrorMessage, StartTime, EndTime, DataFreshness)
        VALUES 
            (@RunID, 'PRJobRev', 'Curated', 'UPSERT', @SourceRows, @InsertedRows, @UpdatedRows, 'SUCCESS', NULL, @StartTime, @EndTime, @DataFreshnessTime);

        PRINT 'SUCCESS: Curated layer processed. New rows: ' + CAST(@InsertedRows AS VARCHAR) + ' | Updated rows: ' + CAST(@UpdatedRows AS VARCHAR);

    END TRY
    BEGIN CATCH
        -- F. Handle errors and log the failure
        SET @EndTime = GETDATE();
        SET @ErrorMsg = ERROR_MESSAGE();

        INSERT INTO dbo.SyncRunLog 
            (RunID, DatasetName, Layer, ActionType, SourceRowCount, InsertedCount, UpdatedCount, Status, ErrorMessage, StartTime, EndTime, DataFreshness)
        VALUES 
            (@RunID, 'PRJobRev', 'Curated', 'UPSERT', @SourceRows, 0, 0, 'FAILED', @ErrorMsg, @StartTime, @EndTime, @DataFreshnessTime);
            
        PRINT 'ERROR: Curated process failed. Message: ' + @ErrorMsg;
        THROW;
    END CATCH
END;
GO