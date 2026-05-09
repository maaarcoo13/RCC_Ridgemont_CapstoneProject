-- One time Azure set up
IF NOT EXISTS (
    SELECT 1 FROM sys.schemas WHERE name = 'curated'
)
BEGIN
    EXEC('CREATE SCHEMA curated');
END
GO

IF OBJECT_ID('curated.zvAPVMCo1', 'U') IS NULL
BEGIN
    CREATE TABLE curated.zvAPVMCo1
    (
        VendorGroup INT NULL,
        Vendor INT NULL,
        Co INT NULL,
        Name NVARCHAR(50) NULL,
        EFT NVARCHAR(5) NULL,
        udExempt NVARCHAR(5) NULL,
        SortName NVARCHAR(50) NULL,
        GLCo INT NULL,
        GLAcct NVARCHAR(50) NULL,
        ActiveYN NVARCHAR(5) NULL,
        LastInvDate DATETIME NULL
    );
END
GO
[zvAPVMCo1_Automation]

---- sanity check
select TOP 10*
from curated.zvAPVMCo1;

SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'curated'
  AND TABLE_NAME = 'zvAPVMCo1'
ORDER BY ORDINAL_POSITION;

-- Centralized cloud logging table
SELECT * FROM [curated].[CentralPipelineLog] 
order by DatasetName

SELECT TOP 20 *
FROM curated.CentralPipelineLog
ORDER BY StartTime DESC;


-- Some Fixes
DELETE FROM curated.CentralPipelineLog WHERE DatasetName = 'Cloud Push';

select * from curated.zvAPVMCo1;

-- business key null check
SELECT
    SUM(CASE WHEN VendorGroup IS NULL THEN 1 ELSE 0 END) AS Null_VendorGroup,
    SUM(CASE WHEN Vendor IS NULL THEN 1 ELSE 0 END) AS Null_Vendor,
    SUM(CASE WHEN Co IS NULL THEN 1 ELSE 0 END) AS Null_Co
FROM curated.zvAPVMCo1;

-- business key duplicate check
SELECT VendorGroup, Vendor, Co, COUNT(*) AS Cnt
FROM curated.zvAPVMCo1
GROUP BY VendorGroup, Vendor, Co
HAVING COUNT(*) > 1;

-- Centralized logging table Schema check
SELECT COLUMN_NAME, DATA_TYPE, COLUMN_DEFAULT, IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_SCHEMA = 'curated'
AND TABLE_NAME = 'CentralPipelineLog'
ORDER BY ORDINAL_POSITION;

-- Delete my datasets from Centralized logging table
DELETE FROM curated.CentralPipelineLog
WHERE DatasetName IN ('zvAPVMCo1', 'zvOHAllocationCalc2'); SELECT * FROM curated.CentralPipelineLog
WHERE DatasetName IN ('zvAPVMCo1', 'zvOHAllocationCalc2')
ORDER BY StartTime DESC;


select count(*) as totalrows
from curated.zvAPVMCo1;

SELECT *
FROM curated.CentralPipelineLog
WHERE DatasetName = 'zvAPVMCo1'
AND RunID = 'RUN_20260427_230005'
ORDER BY LogID;