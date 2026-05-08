@echo off
echo =======================================================
echo STARTING zvPFCbyJob DATA PIPELINE...
echo =======================================================

echo 1. Exporting data from local TANVI server...
sqlcmd -S TANVI -d Ridgemont_SyncStore -E -C -Q "SELECT JCCo,Contract,Department,ISNULL(Description,''),ISNULL(udCommonCust,''),RevOrigAmt,RevCurAmt,COAmt,CostActualAmt,CostOrigAmt,CostCurAmt,CostProjected,ProfitOrigAmt,ProfitCurAmt,ProfitActual,ActualGP,ProfitProj,ProjectedGP,BilledAmt,ISNULL(CONVERT(varchar(23),StartMonth,121),''),ISNULL(CONVERT(varchar(23),MonthClosed,121),''),ISNULL(CONVERT(varchar(23),ProjCloseDate,121),''),JobStatus,ISNULL(CAST(Year AS varchar(10)),''),Expr1,ISNULL(MailAddress,''),ISNULL(MailCity,''),ISNULL(MailState,''),ISNULL(CAST(MailZip AS varchar(10)),''),ISNULL(MailAddress2,''),ISNULL(ShipAddress,''),ISNULL(ShipCity,''),ISNULL(ShipState,''),ISNULL(CAST(ShipZip AS varchar(10)),''),ISNULL(ShipAddress2,''),ISNULL(udDirector,''),ISNULL(TeamOrigin,0),ISNULL(TeamProj,0),ISNULL(TeamActual,0),ISNULL(ProfitEffOrig,0),ISNULL(ProfitEffProj,0),ISNULL(ProfitEffActual,0),ISNULL(CONVERT(varchar(23),CreatedDate,121),''),ISNULL(CONVERT(varchar(23),LastModifiedDate,121),'') FROM curated.zvPFCbyJob" -o "C:\Exports\curated_export.csv" -s"|" -W -h-1 -w 65535
if %errorlevel% neq 0 (
    echo ERROR: Export failed! Pipeline stopped.
    pause
    exit /b %errorlevel%
)

echo Checking export file...
dir C:\Exports\curated_export.csv
dir C:\EXPORTS\curated_export.csv

echo.
echo 2. Wiping existing data from Azure...
sqlcmd -S tcp:rcc-capstone.database.windows.net,1433 -d RCC_Capstone -U zsql -P "RCC1520!sql" -b -l 60 -t 60 -Q "DELETE FROM curated.zvPFCbyJob"
if %errorlevel% neq 0 (
    echo ERROR: Delete failed! Pipeline stopped.
    pause
    exit /b %errorlevel%
)

echo.
echo 3. Importing CSV into Azure...
"C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\180\Tools\Binn\bcp.exe" curated.zvPFCbyJob in "C:\Exports\curated_export.csv" -S tcp:rcc-capstone.database.windows.net,1433 -d RCC_Capstone -U zsql -P "RCC1520!sql" -c -t"|" -F 2 -l 60 -e "C:\Exports\bcp_errors.txt" -m 1
if %errorlevel% neq 0 (
    echo ERROR: Import failed! Pipeline stopped.
    pause
    exit /b %errorlevel%
)


echo.
echo =======================================================
echo PIPELINE COMPLETE! AZURE TABLE UPDATED.
echo =======================================================
pause