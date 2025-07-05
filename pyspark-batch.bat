@echo off
echo Running papermill...

"C:\Users\ADMIN\AppData\Roaming\Python\Python310\Scripts\papermill.exe" "E:\01. Python Basic\cricket_stream\cricket_stream_test.ipynb" "E:\01. Python Basic\cricket_stream\executed\output_test_from_bat.ipynb" -k python3 >> "E:\01. Python Basic\cricket_stream\logs\papermill_run.log" 2>&1

echo Papermill finished with exit code %ERRORLEVEL%
echo Check log file for details.
pause