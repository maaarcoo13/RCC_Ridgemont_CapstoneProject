@echo off
echo =====================================
echo STARTING PIPELINE
echo =====================================

echo.
echo 1. Running pipeline...
sqlcmd -S TANVI -d Ridgemont_SyncStore -E -C -Q "EXEC ops.Run_Job_Pipeline"
if %errorlevel% neq 0 (
    echo ERROR: Pipeline failed!
    pause
    exit /b %errorlevel%
)

echo.
echo 2. Exporting logging table to CSV...
sqlcmd -S TANVI -d Ridgemont_SyncStore -E -C -Q "SELECT LogID,RunID,DatasetName,Layer,ActionType,SourceRowCount,InsertedCount,UpdatedCount,Status,ISNULL(ErrorMessage,''),ISNULL(CONVERT(varchar(19),StartTime,121),''),ISNULL(CONVERT(varchar(19),EndTime,121),''),ISNULL(CONVERT(varchar(19),DataFreshness,121),'') FROM ops.SyncRunLog" -o "C:\Exports\SyncRunLog.csv" -s"|" -W -h-1 -w 65535
if %errorlevel% neq 0 (
    echo ERROR: Log export failed!
    pause
    exit /b %errorlevel%
)

echo.
echo 3. Wiping existing logs on Azure...
sqlcmd -S tcp:rcc-capstone.database.windows.net,1433 -d RCC_Capstone -U zsql -P "RCC1520!sql" -b -l 60 -t 60 -Q "DELETE FROM ops.SyncRunLog"
if %errorlevel% neq 0 (
    echo ERROR: Azure log wipe failed!
    pause
    exit /b %errorlevel%
)

echo.
echo 4. Uploading logs to Azure...
"C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\180\Tools\Binn\bcp.exe" ops.SyncRunLog in "C:\Exports\SyncRunLog.csv" -S tcp:rcc-capstone.database.windows.net,1433 -d RCC_Capstone -U zsql -P "RCC1520!sql" -c -t"|" -F 2 -l 60 -k -e "C:\Exports\log_bcp_errors.txt" -m 100
if %errorlevel% neq 0 (
    echo ERROR: Log upload failed!
    pause
    exit /b %errorlevel%
)

echo.
echo =====================================
echo PIPELINE COMPLETE
echo =====================================
pause