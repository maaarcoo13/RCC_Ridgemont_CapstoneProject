CREATE OR ALTER PROCEDURE dbo.sp_LoadLanding_PRJobRev
    @RunID VARCHAR(100) = 'MANUAL_LOCAL_RUN'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @StartTime DATETIME = GETDATE();
    DECLARE @EndTime DATETIME;
    DECLARE @ErrorMsg VARCHAR(MAX);
    DECLARE @InsertedRows INT = 0;

    BEGIN TRY
        -- A) Limpiar Landing (Siempre es un borrado completo y carga nueva)
        TRUNCATE TABLE landing.PRJobRev;

        -- B) Importar el CSV directamente a Landing
        BULK INSERT landing.PRJobRev
        FROM '/var/opt/mssql/data/PRJobRev.csv' 
        WITH (
            FORMAT = 'CSV',
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\r\n', 
            TABLOCK
        );

        SET @InsertedRows = @@ROWCOUNT;
        SET @EndTime = GETDATE();

        -- C) Registrar en la tabla de logs (Fíjate en Layer = 'Landing')
        INSERT INTO dbo.SyncRunLog (RunID, DatasetName, Layer, ActionType, SourceRowCount, InsertedCount, UpdatedCount, Status, ErrorMessage, StartTime, EndTime)
        VALUES (@RunID, 'PRJobRev', 'Landing', 'BULK INSERT', @InsertedRows, @InsertedRows, 0, 'SUCCESS', NULL, @StartTime, @EndTime);

        PRINT 'SUCCESS: Rows inserted into Landing: ' + CAST(@InsertedRows AS VARCHAR);

    END TRY
    BEGIN CATCH
        SET @ErrorMsg = ERROR_MESSAGE();
        SET @EndTime = GETDATE();
        
        INSERT INTO dbo.SyncRunLog (RunID, DatasetName, Layer, ActionType, SourceRowCount, InsertedCount, UpdatedCount, Status, ErrorMessage, StartTime, EndTime)
        VALUES (@RunID, 'PRJobRev', 'Landing', 'ERROR', 0, 0, 0, 'FAILED', @ErrorMsg, @StartTime, @EndTime);
        
        PRINT 'CRITICAL ERROR in Landing: ' + @ErrorMsg;
        THROW;
    END CATCH
END;
GO