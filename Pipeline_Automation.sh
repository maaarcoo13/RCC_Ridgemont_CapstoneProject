#!/bin/bash

# Set PATH so cron can find installed programs on Mac (like Docker)
export PATH=/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:/usr/sbin:/sbin:$PATH

# Get current date and time
START_TIME=$(date '+%Y-%m-%d %H:%M:%S')

echo "================================================================"
echo "📅 PIPELINE RAN AT: $START_TIME"
echo "================================================================"

# 1. Generate a unique RunID for this execution
RUNID="RUN_$(date +%Y%m%d_%H%M%S)"
echo "🚀 Starting Hybrid Pipeline - RunID: $RUNID"

# 2. Execute local processing across the 3 layers (Landing -> Staging -> Curated)
echo "▶️ STEP 1: Executing local procedures (Landing, Staging, Curated)..."
docker exec sqlserver /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'StrongPasscd1!' -Q "
    EXEC CapstoneProject.dbo.sp_LoadLanding_PRJobRev @RunID='$RUNID';
    EXEC CapstoneProject.dbo.sp_LoadStaging_PRJobRev @RunID='$RUNID';
    EXEC CapstoneProject.dbo.sp_LoadCurated_PRJobRev @RunID='$RUNID';
" -C

# 3. Export Curated data and the current Logs to CSV
echo "▶️ STEP 2: Exporting Curated data and current Logs to CSV..."

# Export Curated Data
docker exec sqlserver /opt/mssql-tools18/bin/bcp "SELECT * FROM CapstoneProject.curated.PRJobRev" queryout "/var/opt/mssql/data/curated_export.csv" -c -t, -S localhost -U sa -P 'StrongPasscd1!' -u

# Export Logs: We insert "0 AS LogID" so the CSV has exactly 13 columns.
docker exec sqlserver /opt/mssql-tools18/bin/bcp "SELECT 0 AS LogID, RunID, DatasetName, Layer, ActionType, SourceRowCount, InsertedCount, UpdatedCount, Status, ErrorMessage, StartTime, EndTime, DataFreshness FROM CapstoneProject.dbo.SyncRunLog WHERE RunID = '$RUNID'" queryout "/var/opt/mssql/data/current_logs.csv" -c -t, -S localhost -U sa -P 'StrongPasscd1!' -u

# 4. Clean old data in Azure (Only the data table, NOT the logs table)
echo "▶️ STEP 3: Cleaning Curated table in Azure..."
docker exec sqlserver /opt/mssql-tools18/bin/sqlcmd -S tcp:rcc-capstone.database.windows.net,1433 -d RCC_Capstone -U zsql -P 'RCC1520!sql' -Q "DELETE FROM curated.PRJobRev;" -C

# 5. Upload new data and append logs to the cloud
echo "▶️ STEP 4: Syncing with the cloud (Azure SQL)..."

# Upload Data to Curated
docker exec sqlserver /opt/mssql-tools18/bin/bcp "curated.PRJobRev" in "/var/opt/mssql/data/curated_export.csv" -d RCC_Capstone -c -t, -S tcp:rcc-capstone.database.windows.net,1433 -U zsql -P 'RCC1520!sql' -u

# Get completion time
END_TIME=$(date '+%Y-%m-%d %H:%M:%S')

echo "✅ Pipeline completed successfully at $END_TIME!"
echo "Execution ID: $RUNID"
echo "================================================================"
