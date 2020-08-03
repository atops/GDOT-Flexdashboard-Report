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
cd c:\users\V0010894\Code\GDOT\scheduled_tasks
python get_det_config_maxtime2.py >> logs\get_maxtime2_%datestr%.log

rem Clear Chrome cache
for /f %%1 in ('dir /b C:\Users\V0010894\AppData\Local\Temp') do rd /s /q C:\Users\V0010894\AppData\Local\Temp\%%1