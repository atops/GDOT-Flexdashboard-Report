echo %date%%time%
for /f "tokens=1-4 delims=/ " %%i in ("%date%") do (
     set dow=%%i
     set month=%%j
     set day=%%k
     set year=%%l
)
set datestr=%year%%month%%day%
echo datestr is %datestr%

call c:\users\V0010894\Anaconda3\Scripts\activate.bat
cd C:\Users\V0010894\Code\GDOT\scheduled_tasks
python get_watchdog_alerts.py >> logs\get_watchdog_alerts_%datestr%.log
Rscript get_alerts.R
