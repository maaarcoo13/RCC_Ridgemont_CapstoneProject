CREATE OR ALTER PROCEDURE dbo.sp_LoadStaging_PRJobRev
    @RunID VARCHAR(100) = 'MANUAL_LOCAL_RUN'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @EndTime DATETIME;
    DECLARE @ErrorMsg VARCHAR(MAX);
    DECLARE @InsertedRows INT = 0;
    DECLARE @SourceRows INT = 0;

    BEGIN TRY
        -- A) Contar filas en Landing
        SELECT @SourceRows = COUNT(*) FROM landing.PRJobRev;

        -- B) Limpiar la tabla Staging actual
        TRUNCATE TABLE dbo.stg_PRJobRev;

        -- C) Mover datos de Landing a Staging (Aquí SQL Server hará el CAST implícito de VARCHAR a INT/DATETIME)
        INSERT INTO dbo.stg_PRJobRev (Job, JCDept, BillYear, BillMonth, [Date], TotalHours, BilledAmt, Retainage, BilledOwed, Description)
        SELECT 
            NULLIF(Job, ''),
            NULLIF(JCDept, ''),
            TRY_CAST(BillYear AS INT),
            TRY_CAST(BillMonth AS INT),
            TRY_CAST([Date] AS DATETIME),
            TRY_CAST(TotalHours AS FLOAT),
            TRY_CAST(BilledAmt AS FLOAT),
            TRY_CAST(Retainage AS FLOAT),
            TRY_CAST(BilledOwed AS FLOAT),
            Description
        FROM landing.PRJobRev;

        SET @InsertedRows = @@ROWCOUNT;
        SET @EndTime = GETDATE();

        -- D) Registrar éxito (Cambiado de 'INSERT' a 'Full Load')
        INSERT INTO dbo.SyncRunLog (RunID, DatasetName, Layer, ActionType, SourceRowCount, InsertedCount, UpdatedCount, Status, ErrorMessage, StartTime, EndTime)
        VALUES (@RunID, 'PRJobRev', 'Staging', 'Full Load', @SourceRows, @InsertedRows, 0, 'SUCCESS', NULL, @StartTime, @EndTime);

        PRINT 'SUCCESS: Rows processed from Landing to Staging: ' + CAST(@InsertedRows AS VARCHAR);

    END TRY
    BEGIN CATCH
        SET @ErrorMsg = ERROR_MESSAGE();
        SET @EndTime = GETDATE();
        
        -- E) Registrar error (También marcado como 'Full Load' para mantener consistencia)
        INSERT INTO dbo.SyncRunLog (RunID, DatasetName, Layer, ActionType, SourceRowCount, InsertedCount, UpdatedCount, Status, ErrorMessage, StartTime, EndTime)
        VALUES (@RunID, 'PRJobRev', 'Staging', 'Full Load', @SourceRows, 0, 0, 'FAILED', @ErrorMsg, @StartTime, @EndTime);
        
        PRINT 'CRITICAL ERROR in Staging: ' + @ErrorMsg;
        THROW;
    END CATCH
END;
GO