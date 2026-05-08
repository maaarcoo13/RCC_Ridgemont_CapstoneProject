-- ==========================================
-- 1. CREATE LOCAL DATABASE
-- ==========================================
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'Capstone')
BEGIN
    CREATE DATABASE Capstone;
END
GO

USE Capstone;
GO

-- ==========================================
-- 2. CREATE SCHEMAS (Added 'lnd' layer)
-- ==========================================
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'lnd')
BEGIN
    EXEC('CREATE SCHEMA [lnd]');
END
GO
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'stg')
BEGIN
    EXEC('CREATE SCHEMA [stg]');
END
GO
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'curated')
BEGIN
    EXEC('CREATE SCHEMA [curated]');
END
GO

-- ==========================================
-- 3. TEAR DOWN OLD TABLES & CREATE NEW ONES
-- ==========================================
DROP TABLE IF EXISTS [curated].[zvPRPreconHistory];
DROP TABLE IF EXISTS [curated].[PRPreconWeightedTotals];
DROP TABLE IF EXISTS [curated].[zvPRPrecon];
DROP TABLE IF EXISTS [stg].[PreconHistoryUpdate];
DROP TABLE IF EXISTS [stg].[PreconWeightedUpdate];
DROP TABLE IF EXISTS [stg].[PreconPrimaryUpdate];
DROP TABLE IF EXISTS [lnd].[PreconHistoryUpdate];
DROP TABLE IF EXISTS [lnd].[PreconWeightedUpdate];
DROP TABLE IF EXISTS [lnd].[PreconPrimaryUpdate];
DROP TABLE IF EXISTS [curated].[CentralPipelineLog];
GO

-- ----------------------------------------------------
-- A. LANDING LAYER (Raw text safety net - All VARCHAR)
-- ----------------------------------------------------
CREATE TABLE [lnd].[PreconPrimaryUpdate] (
    GroupPosition VARCHAR(MAX), PreconCat VARCHAR(MAX), RecordCount VARCHAR(MAX), AvgHourlySalary VARCHAR(MAX), MaxHourlySalary VARCHAR(MAX), MinHourlySalary VARCHAR(MAX), StdDevHourlySalary VARCHAR(MAX), AvgHourlyAuto VARCHAR(MAX), MaxHourlyAuto VARCHAR(MAX), MinHourlyAuto VARCHAR(MAX), StdDevHourlyAuto VARCHAR(MAX), AvgHourlyTotalComp VARCHAR(MAX), MaxHourlyTotalComp VARCHAR(MAX), MinHourlyTotalComp VARCHAR(MAX), StdDevHourlyTotalComp VARCHAR(MAX), AvgWeeklySalary VARCHAR(MAX), MaxWeeklySalary VARCHAR(MAX), MinWeeklySalary VARCHAR(MAX), STDEVWeeklySalary VARCHAR(MAX), AvgWeeklyAuto VARCHAR(MAX), MaxWeeklyAuto VARCHAR(MAX), MinWeeklyAuto VARCHAR(MAX), STDEVWeeklyAuto VARCHAR(MAX), AvgWeeklyTotalComp VARCHAR(MAX), MaxWeeklyTotalComp VARCHAR(MAX), MinWeeklyTotalComp VARCHAR(MAX), STDEVWeeklyTotalComp VARCHAR(MAX), AvgTenure VARCHAR(MAX), MaxTenure VARCHAR(MAX), MinTenure VARCHAR(MAX), StdDevTenure VARCHAR(MAX), AvgAge VARCHAR(MAX), MaxAge VARCHAR(MAX), MinAge VARCHAR(MAX), StdDevAge VARCHAR(MAX)
);

CREATE TABLE [lnd].[PreconWeightedUpdate] (
    GroupPosition VARCHAR(MAX), PreconCat VARCHAR(MAX), AvgHourlySalary VARCHAR(MAX), MaxHourlySalary VARCHAR(MAX), MinHourlySalary VARCHAR(MAX), StdDevHourlySalary VARCHAR(MAX), AvgHourlyAuto VARCHAR(MAX), MaxHourlyAuto VARCHAR(MAX), MinHourlyAuto VARCHAR(MAX), StdDevHourlyAuto VARCHAR(MAX), WeightedAvgHourlyTotalComp VARCHAR(MAX), MaxHourlyTotalComp VARCHAR(MAX), MinHourlyTotalComp VARCHAR(MAX), WeightedStdDevHourlyTotalComp VARCHAR(MAX), WeightedAvgTenure VARCHAR(MAX), MaxTenure VARCHAR(MAX), MinTenure VARCHAR(MAX), WeightedAvgAge VARCHAR(MAX), MaxAge VARCHAR(MAX), MinAge VARCHAR(MAX)
);

CREATE TABLE [lnd].[PreconHistoryUpdate] (
    Month VARCHAR(MAX), Year VARCHAR(MAX), Description VARCHAR(MAX), PreconCat VARCHAR(MAX), PositionCode VARCHAR(MAX), GroupPosition VARCHAR(MAX), EmployeeCount VARCHAR(MAX), AvgHourlySum VARCHAR(MAX), MaxHourlySum VARCHAR(MAX), MinHourlySum VARCHAR(MAX), StdDevHourlySum VARCHAR(MAX), Calc VARCHAR(MAX), TotalHourlySum VARCHAR(MAX)
);

-- ----------------------------------------------------
-- B. STAGING LAYER (Typed Loading Dock)
-- ----------------------------------------------------
CREATE TABLE [stg].[PreconPrimaryUpdate] (
    GroupPosition VARCHAR(100), PreconCat VARCHAR(100), RecordCount FLOAT, AvgHourlySalary FLOAT, MaxHourlySalary FLOAT, MinHourlySalary FLOAT, StdDevHourlySalary FLOAT, AvgHourlyAuto FLOAT, MaxHourlyAuto FLOAT, MinHourlyAuto FLOAT, StdDevHourlyAuto FLOAT, AvgHourlyTotalComp FLOAT, MaxHourlyTotalComp FLOAT, MinHourlyTotalComp FLOAT, StdDevHourlyTotalComp FLOAT, AvgWeeklySalary FLOAT, MaxWeeklySalary FLOAT, MinWeeklySalary FLOAT, STDEVWeeklySalary FLOAT, AvgWeeklyAuto FLOAT, MaxWeeklyAuto FLOAT, MinWeeklyAuto FLOAT, STDEVWeeklyAuto FLOAT, AvgWeeklyTotalComp FLOAT, MaxWeeklyTotalComp FLOAT, MinWeeklyTotalComp FLOAT, STDEVWeeklyTotalComp FLOAT, AvgTenure FLOAT, MaxTenure FLOAT, MinTenure FLOAT, StdDevTenure FLOAT, AvgAge FLOAT, MaxAge FLOAT, MinAge FLOAT, StdDevAge FLOAT
);

CREATE TABLE [stg].[PreconWeightedUpdate] (
    GroupPosition VARCHAR(100), PreconCat VARCHAR(100), AvgHourlySalary FLOAT, MaxHourlySalary FLOAT, MinHourlySalary FLOAT, StdDevHourlySalary FLOAT, AvgHourlyAuto FLOAT, MaxHourlyAuto FLOAT, MinHourlyAuto FLOAT, StdDevHourlyAuto FLOAT, WeightedAvgHourlyTotalComp FLOAT, MaxHourlyTotalComp FLOAT, MinHourlyTotalComp FLOAT, WeightedStdDevHourlyTotalComp FLOAT, WeightedAvgTenure FLOAT, MaxTenure FLOAT, MinTenure FLOAT, WeightedAvgAge FLOAT, MaxAge FLOAT, MinAge FLOAT
);

CREATE TABLE [stg].[PreconHistoryUpdate] (
    Month INT, Year INT, Description VARCHAR(100), PreconCat VARCHAR(100), PositionCode VARCHAR(100), GroupPosition VARCHAR(100), EmployeeCount FLOAT, AvgHourlySum FLOAT, MaxHourlySum FLOAT, MinHourlySum FLOAT, StdDevHourlySum FLOAT, Calc FLOAT, TotalHourlySum FLOAT
);

-- ----------------------------------------------------
-- C. CURATED LAYER (Permanent Business Tables)
-- ----------------------------------------------------
CREATE TABLE [curated].[zvPRPrecon] (
    GroupPosition VARCHAR(100), PreconCat VARCHAR(100) PRIMARY KEY, RecordCount FLOAT, AvgHourlySalary FLOAT, MaxHourlySalary FLOAT, MinHourlySalary FLOAT, StdDevHourlySalary FLOAT, AvgHourlyAuto FLOAT, MaxHourlyAuto FLOAT, MinHourlyAuto FLOAT, StdDevHourlyAuto FLOAT, AvgHourlyTotalComp FLOAT, MaxHourlyTotalComp FLOAT, MinHourlyTotalComp FLOAT, StdDevHourlyTotalComp FLOAT, AvgWeeklySalary FLOAT, MaxWeeklySalary FLOAT, MinWeeklySalary FLOAT, STDEVWeeklySalary FLOAT, AvgWeeklyAuto FLOAT, MaxWeeklyAuto FLOAT, MinWeeklyAuto FLOAT, STDEVWeeklyAuto FLOAT, AvgWeeklyTotalComp FLOAT, MaxWeeklyTotalComp FLOAT, MinWeeklyTotalComp FLOAT, STDEVWeeklyTotalComp FLOAT, AvgTenure FLOAT, MaxTenure FLOAT, MinTenure FLOAT, StdDevTenure FLOAT, AvgAge FLOAT, MaxAge FLOAT, MinAge FLOAT, StdDevAge FLOAT
);

CREATE TABLE [curated].[PRPreconWeightedTotals] (
    GroupPosition VARCHAR(100), PreconCat VARCHAR(100), AvgHourlySalary FLOAT, MaxHourlySalary FLOAT, MinHourlySalary FLOAT, StdDevHourlySalary FLOAT, AvgHourlyAuto FLOAT, MaxHourlyAuto FLOAT, MinHourlyAuto FLOAT, StdDevHourlyAuto FLOAT, WeightedAvgHourlyTotalComp FLOAT, MaxHourlyTotalComp FLOAT, MinHourlyTotalComp FLOAT, WeightedStdDevHourlyTotalComp FLOAT, WeightedAvgTenure FLOAT, MaxTenure FLOAT, MinTenure FLOAT, WeightedAvgAge FLOAT, MaxAge FLOAT, MinAge FLOAT,
    CONSTRAINT FK_Weighted_PreconCat FOREIGN KEY (PreconCat) REFERENCES [curated].[zvPRPrecon](PreconCat)
);

CREATE TABLE [curated].[zvPRPreconHistory] (
    Month INT, Year INT, Description VARCHAR(100), PreconCat VARCHAR(100), PositionCode VARCHAR(100), GroupPosition VARCHAR(100), EmployeeCount FLOAT, AvgHourlySum FLOAT, MaxHourlySum FLOAT, MinHourlySum FLOAT, StdDevHourlySum FLOAT, Calc FLOAT, TotalHourlySum FLOAT,
    CONSTRAINT FK_History_PreconCat FOREIGN KEY (PreconCat) REFERENCES [curated].[zvPRPrecon](PreconCat)
);

-- ----------------------------------------------------
-- D. LOGGING TABLE
-- ----------------------------------------------------
CREATE TABLE [curated].[CentralPipelineLog] (
    LogID INT IDENTITY(1,1) PRIMARY KEY, RunID VARCHAR(100) NULL, DatasetName VARCHAR(100) NULL, Layer VARCHAR(50) NULL, ActionType VARCHAR(50) NULL, SourceRowCount INT NULL, InsertedCount INT DEFAULT 0 NULL, UpdatedCount INT DEFAULT 0 NULL, Status VARCHAR(50) NULL, ErrorMessage VARCHAR(MAX) NULL, StartTime DATETIME NULL, EndTime DATETIME NULL, DataFreshness DATETIME NULL
);
GO

-- ==========================================
-- 4. THE BOUNCER (Validates the Landing Layer)
-- ==========================================
CREATE OR ALTER PROCEDURE [curated].[usp_Precon_ValidateStaging]
    @RunID VARCHAR(100) = 'Local Run'
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @PrimaryCount INT = (SELECT COUNT(*) FROM [lnd].[PreconPrimaryUpdate]);
    DECLARE @WeightedCount INT = (SELECT COUNT(*) FROM [lnd].[PreconWeightedUpdate]);
    DECLARE @HistoryCount INT = (SELECT COUNT(*) FROM [lnd].[PreconHistoryUpdate]);
    DECLARE @StepStartTime DATETIME = GETDATE();

    BEGIN TRY
        IF (@PrimaryCount = 0 OR @WeightedCount = 0 OR @HistoryCount = 0)
            THROW 50001, 'VALIDATION FAILED: One or more landing files are completely empty.', 1;

        INSERT INTO [curated].[CentralPipelineLog] (RunID, DatasetName, Layer, ActionType, SourceRowCount, InsertedCount, UpdatedCount, Status, ErrorMessage, StartTime, EndTime, DataFreshness)
        VALUES 
        (@RunID, 'PreconPrimary',  'Landing', 'Full Load', @PrimaryCount,  @PrimaryCount,  0, 'SUCCESS', NULL, @StepStartTime, GETDATE(), NULL),
        (@RunID, 'PreconWeighted', 'Landing', 'Full Load', @WeightedCount, @WeightedCount, 0, 'SUCCESS', NULL, @StepStartTime, GETDATE(), NULL),
        (@RunID, 'PreconHistory',  'Landing', 'Full Load', @HistoryCount,  @HistoryCount,  0, 'SUCCESS', NULL, @StepStartTime, GETDATE(), NULL);
    END TRY
    BEGIN CATCH
        INSERT INTO [curated].[CentralPipelineLog] (RunID, DatasetName, Layer, ActionType, SourceRowCount, InsertedCount, UpdatedCount, Status, ErrorMessage, StartTime, EndTime, DataFreshness)
        VALUES 
        (@RunID, 'PreconPrimary',  'Landing', 'Full Load', @PrimaryCount,  0, 0, 'FAILED', ERROR_MESSAGE(), @StepStartTime, GETDATE(), NULL),
        (@RunID, 'PreconWeighted', 'Landing', 'Full Load', @WeightedCount, 0, 0, 'FAILED', ERROR_MESSAGE(), @StepStartTime, GETDATE(), NULL),
        (@RunID, 'PreconHistory',  'Landing', 'Full Load', @HistoryCount,  0, 0, 'FAILED', ERROR_MESSAGE(), @StepStartTime, GETDATE(), NULL);
        THROW; 
    END CATCH;
END;
GO

-- ==========================================
-- 5. THE ENGINE (Landing -> Staging -> Curated)
-- ==========================================
CREATE OR ALTER PROCEDURE [curated].[usp_Precon_RunCuratingPipeline]
    @RunID VARCHAR(100) = 'Local Run' 
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @LndRows_Primary  INT, @LndRows_Weighted INT, @LndRows_History INT;
    DECLARE @StgRows_Primary  INT, @CurRows_Primary  INT;
    DECLARE @StgRows_Weighted INT, @CurRows_Weighted INT;
    DECLARE @StgRows_History  INT, @CurRows_History  INT;
    DECLARE @StgStart_Primary  DATETIME, @StgEnd_Primary  DATETIME;
    DECLARE @CurStart_Primary  DATETIME, @CurEnd_Primary  DATETIME;
    DECLARE @StgStart_Weighted DATETIME, @StgEnd_Weighted DATETIME;
    DECLARE @CurStart_Weighted DATETIME, @CurEnd_Weighted DATETIME;
    DECLARE @StgStart_History  DATETIME, @StgEnd_History  DATETIME;
    DECLARE @CurStart_History  DATETIME, @CurEnd_History  DATETIME;
    DECLARE @ErrorMsg VARCHAR(MAX) = NULL;

    -- Capture landing row counts before transaction begins
    SET @LndRows_Primary  = (SELECT COUNT(*) FROM [lnd].[PreconPrimaryUpdate]);
    SET @LndRows_Weighted = (SELECT COUNT(*) FROM [lnd].[PreconWeightedUpdate]);
    SET @LndRows_History  = (SELECT COUNT(*) FROM [lnd].[PreconHistoryUpdate]);

    BEGIN TRY
        BEGIN TRANSACTION;

        -- ==========================================
        -- DATASET 1: PRIMARY
        -- ==========================================

        -- Landing -> Staging
        SET @StgStart_Primary = GETDATE();
        DELETE FROM [stg].[PreconPrimaryUpdate];
        INSERT INTO [stg].[PreconPrimaryUpdate] SELECT * FROM [lnd].[PreconPrimaryUpdate];
        SET @StgRows_Primary = @@ROWCOUNT;
        SET @StgEnd_Primary = GETDATE();

        -- Staging -> Curated (children deleted first, then parent)
        SET @CurStart_Primary = GETDATE();
        DELETE FROM [curated].[zvPRPreconHistory];
        DELETE FROM [curated].[PRPreconWeightedTotals];
        DELETE FROM [curated].[zvPRPrecon];
        INSERT INTO [curated].[zvPRPrecon] SELECT * FROM [stg].[PreconPrimaryUpdate];
        SET @CurRows_Primary = @@ROWCOUNT;
        SET @CurEnd_Primary = GETDATE();

        -- ==========================================
        -- DATASET 2: WEIGHTED
        -- ==========================================

        -- Landing -> Staging
        SET @StgStart_Weighted = GETDATE();
        DELETE FROM [stg].[PreconWeightedUpdate];
        INSERT INTO [stg].[PreconWeightedUpdate] SELECT * FROM [lnd].[PreconWeightedUpdate];
        SET @StgRows_Weighted = @@ROWCOUNT;
        SET @StgEnd_Weighted = GETDATE();

        -- Staging -> Curated
        SET @CurStart_Weighted = GETDATE();
        INSERT INTO [curated].[PRPreconWeightedTotals] SELECT * FROM [stg].[PreconWeightedUpdate];
        SET @CurRows_Weighted = @@ROWCOUNT;
        SET @CurEnd_Weighted = GETDATE();

        -- ==========================================
        -- DATASET 3: HISTORY
        -- ==========================================

        -- Landing -> Staging
        SET @StgStart_History = GETDATE();
        DELETE FROM [stg].[PreconHistoryUpdate];
        INSERT INTO [stg].[PreconHistoryUpdate] 
            SELECT ISNULL(Month, 0), ISNULL(Year, 0), ISNULL(Description, '0'), ISNULL(PreconCat, '0'), 
                   ISNULL(PositionCode, '0'), ISNULL(GroupPosition, '0'), ISNULL(EmployeeCount, 0), 
                   ISNULL(AvgHourlySum, 0), ISNULL(MaxHourlySum, 0), ISNULL(MinHourlySum, 0), 
                   ISNULL(StdDevHourlySum, 0), ISNULL(Calc, 0), ISNULL(TotalHourlySum, 0) 
            FROM [lnd].[PreconHistoryUpdate];
        SET @StgRows_History = @@ROWCOUNT;
        SET @StgEnd_History = GETDATE();

        -- Staging -> Curated
        SET @CurStart_History = GETDATE();
        INSERT INTO [curated].[zvPRPreconHistory] SELECT * FROM [stg].[PreconHistoryUpdate];
        SET @CurRows_History = @@ROWCOUNT;
        SET @CurEnd_History = GETDATE();

        -- ==========================================
        -- CLEANUP
        -- ==========================================
        DELETE FROM [lnd].[PreconPrimaryUpdate];
        DELETE FROM [lnd].[PreconWeightedUpdate];
        DELETE FROM [lnd].[PreconHistoryUpdate];
        DELETE FROM [stg].[PreconPrimaryUpdate];
        DELETE FROM [stg].[PreconWeightedUpdate];
        DELETE FROM [stg].[PreconHistoryUpdate];

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        SET @ErrorMsg = ERROR_MESSAGE();
        THROW;
    END CATCH;

    -- ==========================================
    -- LOG ALL 6 ENTRIES AFTER TRANSACTION
    -- (Outside transaction so they always survive)
    -- ==========================================
    INSERT INTO [curated].[CentralPipelineLog] (RunID, DatasetName, Layer, ActionType, SourceRowCount, InsertedCount, UpdatedCount, Status, ErrorMessage, StartTime, EndTime, DataFreshness)
    VALUES
        (@RunID, 'PreconPrimary',  'Staging', 'Full Load', @LndRows_Primary,  @StgRows_Primary,  0,                'SUCCESS', NULL, @StgStart_Primary,  @StgEnd_Primary,  NULL),
        (@RunID, 'PreconPrimary',  'Curated', 'Upsert',    @LndRows_Primary,  0, @CurRows_Primary,                 'SUCCESS', NULL, @CurStart_Primary,  @CurEnd_Primary,  GETDATE()),
        (@RunID, 'PreconWeighted', 'Staging', 'Full Load', @LndRows_Weighted, @StgRows_Weighted, 0,                'SUCCESS', NULL, @StgStart_Weighted, @StgEnd_Weighted, NULL),
        (@RunID, 'PreconWeighted', 'Curated', 'Upsert',    @LndRows_Weighted, 0, @CurRows_Weighted,                'SUCCESS', NULL, @CurStart_Weighted, @CurEnd_Weighted, GETDATE()),
        (@RunID, 'PreconHistory',  'Staging', 'Full Load', @LndRows_History,  @StgRows_History,  0,                'SUCCESS', NULL, @StgStart_History,  @StgEnd_History,  NULL),
        (@RunID, 'PreconHistory',  'Curated', 'Upsert',    @LndRows_History,  0, @CurRows_History,                 'SUCCESS', NULL, @CurStart_History,  @CurEnd_History,  GETDATE());
END;
GO