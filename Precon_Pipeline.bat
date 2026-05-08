@echo off
set SERVER=.
set DB=Capstone

echo =======================================================
echo STARTING HYBRID EDGE-TO-CLOUD PIPELINE
echo =======================================================

:: Generate a unique RunID using PowerShell
FOR /F "tokens=*" %%g IN ('powershell -Command "Get-Date -Format 'yyyyMMdd_HHmmss'"') do (SET RunID=RUN_%%g)
echo Execution ID: %RunID%

echo.
echo 1. Wiping Local Landing Tables...
sqlcmd -S %SERVER% -d %DB% -E -C -b -Q "DELETE FROM lnd.PreconPrimaryUpdate; DELETE FROM lnd.PreconWeightedUpdate; DELETE FROM lnd.PreconHistoryUpdate;"

echo.
echo 2. Converting Files and Fixing Text...
powershell -Command "Get-Content 'C:\PR_Precon_Data\Primary.csv' | Where-Object { $_.Trim() -ne '' } | ForEach-Object { $_ -replace ',', '.' -replace ';', ',' } | Set-Content 'C:\PR_Precon_Data\Primary_US.csv' -Encoding ASCII"
powershell -Command "Get-Content 'C:\PR_Precon_Data\Weighted.csv' | Where-Object { $_.Trim() -ne '' } | ForEach-Object { $_ -replace ',', '.' -replace ';', ',' } | Set-Content 'C:\PR_Precon_Data\Weighted_US.csv' -Encoding ASCII"
powershell -Command "Get-Content 'C:\PR_Precon_Data\History.csv' | Where-Object { $_.Trim() -ne '' } | ForEach-Object { $_ -replace ',', '.' -replace ';', ',' } | Set-Content 'C:\PR_Precon_Data\History_US.csv' -Encoding ASCII"

echo.
echo 3. Pushing CSV Files to Local Landing Area...
bcp "lnd.PreconPrimaryUpdate" in "C:\PR_Precon_Data\Primary_US.csv" -S %SERVER% -d %DB% -T -u -c -t "," -F 2
bcp "lnd.PreconWeightedUpdate" in "C:\PR_Precon_Data\Weighted_US.csv" -S %SERVER% -d %DB% -T -u -c -t "," -F 2
bcp "lnd.PreconHistoryUpdate" in "C:\PR_Precon_Data\History_US.csv" -S %SERVER% -d %DB% -T -u -c -t "," -F 2

echo.
echo 4. Running Data Validation Bouncer (Local)...
sqlcmd -S %SERVER% -d %DB% -E -C -b -Q "EXEC curated.usp_Precon_ValidateStaging @RunID='%RunID%';"
if %errorlevel% neq 0 (
    echo =======================================================
    echo ERROR: VALIDATION FAILED! Check the CSV files. 
    echo =======================================================
    pause
    exit /b %errorlevel%
)

echo.
echo 5. Curating Local Permanent Tables...
sqlcmd -S %SERVER% -d %DB% -E -C -b -Q "EXEC curated.usp_Precon_RunCuratingPipeline @RunID='%RunID%';"

echo.
echo =======================================================
echo LOCAL PROCESSING COMPLETE. INITIATING CLOUD SYNC...
echo =======================================================

echo.
echo 6. Exporting Processed Data AND Logs from Local Database...
bcp "curated.zvPRPrecon" out "C:\PR_Precon_Data\CloudPush_Primary.csv" -S %SERVER% -d %DB% -T -u -c -t ","
bcp "curated.PRPreconWeightedTotals" out "C:\PR_Precon_Data\CloudPush_Weighted.csv" -S %SERVER% -d %DB% -T -u -c -t ","
bcp "curated.zvPRPreconHistory" out "C:\PR_Precon_Data\CloudPush_History.csv" -S %SERVER% -d %DB% -T -u -c -t ","
bcp "SELECT RunID, DatasetName, Layer, ActionType, SourceRowCount, InsertedCount, UpdatedCount, Status, ErrorMessage, StartTime, EndTime, DataFreshness FROM curated.CentralPipelineLog WHERE RunID = '%RunID%'" queryout "C:\PR_Precon_Data\CloudPush_Logs.csv" -S %SERVER% -d %DB% -T -u -c -t ","

echo.
echo 7. Wiping Old Cloud Tables and Creating Temporary Log Staging...
sqlcmd -S tcp:rcc-capstone.database.windows.net,1433 -d RCC_Capstone -U zsql -P RCC1520!sql -b -Q "DELETE FROM curated.zvPRPreconHistory; DELETE FROM curated.PRPreconWeightedTotals; DELETE FROM curated.zvPRPrecon; IF OBJECT_ID('curated.CentralPipelineLog_TempStaging_Precon', 'U') IS NOT NULL DROP TABLE curated.CentralPipelineLog_TempStaging_Precon; CREATE TABLE curated.CentralPipelineLog_TempStaging_Precon (RunID VARCHAR(100), DatasetName VARCHAR(100), Layer VARCHAR(50), ActionType VARCHAR(50), SourceRowCount INT, InsertedCount INT, UpdatedCount INT, Status VARCHAR(50), ErrorMessage VARCHAR(MAX), StartTime DATETIME, EndTime DATETIME, DataFreshness DATETIME);"

echo.
echo 8. Pushing Curated Data and Local Logs to Azure Cloud...
bcp "curated.zvPRPrecon" in "C:\PR_Precon_Data\CloudPush_Primary.csv" -S tcp:rcc-capstone.database.windows.net,1433 -d RCC_Capstone -U zsql -P RCC1520!sql -c -t ","
bcp "curated.PRPreconWeightedTotals" in "C:\PR_Precon_Data\CloudPush_Weighted.csv" -S tcp:rcc-capstone.database.windows.net,1433 -d RCC_Capstone -U zsql -P RCC1520!sql -c -t ","
bcp "curated.zvPRPreconHistory" in "C:\PR_Precon_Data\CloudPush_History.csv" -S tcp:rcc-capstone.database.windows.net,1433 -d RCC_Capstone -U zsql -P RCC1520!sql -c -t ","
bcp "curated.CentralPipelineLog_TempStaging_Precon" in "C:\PR_Precon_Data\CloudPush_Logs.csv" -S tcp:rcc-capstone.database.windows.net,1433 -d RCC_Capstone -U zsql -P RCC1520!sql -c -t ","

echo.
echo 9. Merging Logs Safely into Shared Table...
sqlcmd -S tcp:rcc-capstone.database.windows.net,1433 -d RCC_Capstone -U zsql -P RCC1520!sql -Q "INSERT INTO curated.CentralPipelineLog (RunID, DatasetName, Layer, ActionType, SourceRowCount, InsertedCount, UpdatedCount, Status, ErrorMessage, StartTime, EndTime, DataFreshness) SELECT RunID, DatasetName, Layer, ActionType, SourceRowCount, InsertedCount, UpdatedCount, Status, ErrorMessage, StartTime, EndTime, DataFreshness FROM curated.CentralPipelineLog_TempStaging_Precon; DROP TABLE curated.CentralPipelineLog_TempStaging_Precon;"

echo.
echo =======================================================
echo PIPELINE COMPLETE! AZURE CLOUD HAS BEEN UPDATED.
echo =======================================================
pause
