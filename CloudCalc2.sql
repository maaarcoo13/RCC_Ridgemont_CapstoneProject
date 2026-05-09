-- One time Azure set up
IF NOT EXISTS (
    SELECT 1 FROM sys.schemas WHERE name = 'curated'
)
BEGIN
    EXEC('CREATE SCHEMA curated');
END
GO

IF OBJECT_ID('curated.zvOHAllocationCalc2', 'U') IS NULL
BEGIN
    CREATE TABLE curated.zvOHAllocationCalc2
    (
        Job         NVARCHAR(50)    NULL,
        JCDept      NVARCHAR(50)    NULL,
        BillYear    FLOAT             NULL,
        BillMonth   FLOAT             NULL,
        Date        DATETIME        NULL,
        TotalHours  FLOAT           NULL,
        BilledAmt   FLOAT           NULL,
        Retainage   FLOAT           NULL,
        BilledOwed  FLOAT           NULL
    );
END
GO

-- Sanity check
SELECT TOP 10 * FROM curated.zvOHAllocationCalc2;

SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'curated'
  AND TABLE_NAME = 'zvOHAllocationCalc2'
ORDER BY ORDINAL_POSITION;