-- zvAPVMCo1-- 
USE RCC_Datastore;
GO

/* 1. Create landing raw table from source if it does not exist */
IF OBJECT_ID('landing.zvAPVMCo1_raw', 'U') IS NULL
BEGIN
    SELECT TOP (0) *
    INTO landing.zvAPVMCo1_raw
    FROM RCC_Source.dbo.zvAPVMCo1;
END
GO

/* 2. Add metadata columns for historical raw tracking */
IF COL_LENGTH('landing.zvAPVMCo1_raw', 'LoadDate') IS NULL
BEGIN
    ALTER TABLE landing.zvAPVMCo1_raw
    ADD LoadDate DATETIME NULL;
END
GO

IF COL_LENGTH('landing.zvAPVMCo1_raw', 'RunID') IS NULL
BEGIN
    ALTER TABLE landing.zvAPVMCo1_raw
    ADD RunID INT NULL;
END
GO

/* 3. Create staging table if it does not exist */
IF OBJECT_ID('stg.zvAPVMCo1', 'U') IS NULL
BEGIN
    SELECT TOP (0) *
    INTO stg.zvAPVMCo1
    FROM RCC_Source.dbo.zvAPVMCo1;
END
GO

/* 4. Create curated table if it does not exist */
IF OBJECT_ID('dbo.zvAPVMCo1', 'U') IS NULL
BEGIN
    SELECT TOP (0) *
    INTO dbo.zvAPVMCo1
    FROM RCC_Source.dbo.zvAPVMCo1;
END
GO

/*==============================================
================= Pipieline starts ==============================*/

USE RCC_Datastore;
GO

CREATE OR ALTER PROCEDURE dbo.sp_Run_zvAPVMCo1_Pipeline
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

        /* 1. Count source rows */
        SET @Step = 'Count source rows';

        SELECT @SourceRowCount = COUNT(*)
        FROM RCC_Source.dbo.zvAPVMCo1;

        /* Refresh landing */

        SET @Step = 'Landing';
        SET @StepStart = GETDATE();


        TRUNCATE TABLE landing.zvAPVMCo1_raw;

        INSERT INTO landing.zvAPVMCo1_raw
        (
            VendorGroup,
            Vendor,
            Co,
            Name,
            EFT,
            udExempt,
            SortName,
            GLCo,
            GLAcct,
            ActiveYN,
            LastInvDate,
            LoadDate,
            RunID
        )
        SELECT
            VendorGroup,
            Vendor,
            Co,
            Name,
            EFT,
            udExempt,
            SortName,
            GLCo,
            GLAcct,
            ActiveYN,
            LastInvDate,
            GETDATE(),
            @RunID
        FROM RCC_Source.dbo.zvAPVMCo1;

        SELECT @LandingRowCount = COUNT(*) FROM landing.zvAPVMCo1_raw;

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
            'zvAPVMCo1',
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
           
        /* 3. Load staging */
        SET @Step = 'Staging';
        SET @StepStart = GETDATE();

        TRUNCATE TABLE stg.zvAPVMCo1;

        INSERT INTO stg.zvAPVMCo1
        (
            VendorGroup,
            Vendor,
            Co,
            Name,
            EFT,
            udExempt,
            SortName,
            GLCo,
            GLAcct,
            ActiveYN,
            LastInvDate
        )
        SELECT 
            VendorGroup,
            Vendor,
            Co,
            Name,
            EFT,
            udExempt,
            SortName,
            GLCo,
            GLAcct,
            ActiveYN,
            LastInvDate
        FROM landing.zvAPVMCo1_raw
        WHERE VendorGroup IS NOT NULL
          AND Vendor IS NOT NULL
          AND Co IS NOT NULL;

        SELECT @StagingRowCount = COUNT(*) FROM stg.zvAPVMCo1;

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
            'zvAPVMCo1',
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

        /* 4. Merge into local curated */
        SET @Step = 'Merge Curated';
        SET @StepStart = GETDATE();


        MERGE dbo.zvAPVMCo1 AS target
        USING stg.zvAPVMCo1 AS source
          ON target.VendorGroup = source.VendorGroup
         AND target.Vendor      = source.Vendor
         AND target.Co          = source.Co

        WHEN MATCHED AND (
               ISNULL(target.Name, '') <> ISNULL(source.Name, '')
            OR ISNULL(target.EFT, '') <> ISNULL(source.EFT, '')
            OR ISNULL(target.udExempt, '') <> ISNULL(source.udExempt, '')
            OR ISNULL(target.SortName, '') <> ISNULL(source.SortName, '')
            OR ISNULL(target.GLCo, -1) <> ISNULL(source.GLCo, -1)
            OR ISNULL(target.GLAcct, -1) <> ISNULL(source.GLAcct, -1)
            OR ISNULL(target.ActiveYN, '') <> ISNULL(source.ActiveYN, '')
            OR ISNULL(target.LastInvDate, '19000101') <> ISNULL(source.LastInvDate, '19000101')
        )
        THEN
            UPDATE SET
                target.Name        = source.Name,
                target.EFT         = source.EFT,
                target.udExempt    = source.udExempt,
                target.SortName    = source.SortName,
                target.GLCo        = source.GLCo,
                target.GLAcct      = source.GLAcct,
                target.ActiveYN    = source.ActiveYN,
                target.LastInvDate = source.LastInvDate

        WHEN NOT MATCHED BY TARGET THEN
            INSERT
            (
                VendorGroup,
                Vendor,
                Co,
                Name,
                EFT,
                udExempt,
                SortName,
                GLCo,
                GLAcct,
                ActiveYN,
                LastInvDate
            )
            VALUES
            (
                source.VendorGroup,
                source.Vendor,
                source.Co,
                source.Name,
                source.EFT,
                source.udExempt,
                source.SortName,
                source.GLCo,
                source.GLAcct,
                source.ActiveYN,
                source.LastInvDate
            )

        OUTPUT $action INTO @MergeResults(MergeAction);

        /* 6. Count actions */
        SET @Step = 'Count merge actions';

        SELECT @InsertedCount = COUNT(*)
        FROM @MergeResults
        WHERE MergeAction = 'INSERT';

        SELECT @UpdatedCount = COUNT(*)
        FROM @MergeResults
        WHERE MergeAction = 'UPDATE';

        -- Log curated layer 
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
            'zvAPVMCo1',
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
            'zvAPVMCo1',
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
=====================================
End of the pipeline
=====================================
*/

 -- run the pipeline 
EXEC dbo.sp_Run_zvAPVMCo1_Pipeline;


-- how many rows on landing
SELECT COUNT(*) AS TotalRaws
FROM landing.zvAPVMCo1_raw;

-- how many rows on staging
SELECT COUNT(*) AS TotalRaws
FROM stg.zvAPVMCo1;

-- how many rows on curated
SELECT COUNT(*) AS TotalRaws
FROM dbo.zvAPVMCo1;

-- Null checks on the buisness key, = 0
SELECT
    COUNT(*) AS TotalRows,
    SUM(CASE WHEN VendorGroup IS NULL THEN 1 ELSE 0 END) AS Null_VendorGroup,
    SUM(CASE WHEN Vendor IS NULL THEN 1 ELSE 0 END) AS Null_Vendor,
    SUM(CASE WHEN Co IS NULL THEN 1 ELSE 0 END) AS Null_Co
FROM landing.zvAPVMCo1_raw;

-- No dupliates in the business key
SELECT
    VendorGroup,
    Vendor,
    Co,
    COUNT(*) AS Cnt
FROM landing.zvAPVMCo1_raw
GROUP BY
    VendorGroup,
    Vendor,
    Co
HAVING COUNT(*) > 1;

-- check null values of every columns
SELECT
    SUM(CASE WHEN Name IS NULL THEN 1 ELSE 0 END) AS NullName,
    SUM(CASE WHEN EFT IS NULL THEN 1 ELSE 0 END) AS NullEFT,
    SUM(CASE WHEN udExempt IS NULL THEN 1 ELSE 0 END) AS NulludExempt,
    SUM(CASE WHEN SortName IS NULL THEN 1 ELSE 0 END) AS NullSortName,
    SUM(CASE WHEN GLCo IS NULL THEN 1 ELSE 0 END) AS NullGLCo,
    SUM(CASE WHEN GLAcct IS NULL THEN 1 ELSE 0 END) AS NullGLAcct,
    SUM(CASE WHEN ActiveYN IS NULL THEN 1 ELSE 0 END) AS NullActiveYN,
    SUM(CASE WHEN LastInvDate IS NULL THEN 1 ELSE 0 END) AS NullLastInvDate
FROM landing.zvAPVMCo1_raw;



SELECT TOP 20 *
FROM dbo.SyncRunLog
ORDER BY RunID DESC;

SELECT TOP 5 RunID, DatasetName, Layer, ActionType, DataFreshness
FROM dbo.SyncRunLog
WHERE DatasetName = 'zvAPVMCo1'
ORDER BY StartTime DESC;

select * from dbo.zvAPVMCo1;

SELECT COUNT(*) AS LcoalcuratedCount
FROM dbo.zvAPVMCo1; 
