
for /f "tokens=1-4 delims=/ " %%i in ("%date%") do (
     set dow=%%i
     set month=%%j
     set day=%%k
     set year=%%l
)
set datestr=%year%%month%%day%
echo datestr is %datestr%

call c:\users\V0010894\Anaconda3\Scripts\activate.bat
cd c:\users\V0010894\Code\GDOT\scheduled_tasks
python pull_raw_atspm_data.py > logs\pull_atspm_data_%datestr%.log
python get_event_gaps.py
rem Delete log files older than 30 days
forfiles /S /M *.log /D -30 /C "cmd /c del @file"

DocumentClient.exe TEAMS_Reports\tasks.csv >> logs\pull_atspm_data_%datestr%.log
zip TEAMS_Reports\tasks.csv.zip TEAMS_Reports\tasks.csv
aws s3 cp TEAMS_Reports\tasks.csv.zip s3://gdot-spm/mark/teams/tasks.csv.zip >> logs\pull_atspm_data_%datestr%.log

rem python pull_atspm_data_wide.py yesterday yesterday