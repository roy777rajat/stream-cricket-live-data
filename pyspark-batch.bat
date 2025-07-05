@echo off
REM Activate virtual environment
call E:\01. Python Basic\cricket_stream\.venv\Scripts\activate.bat

REM Set Spark environment (optional if already in PATH)
set SPARK_HOME=C:\spark\spark-3.5.5-bin-hadoop3
set PATH=%SPARK_HOME%\bin;%PATH%
set HADOOP_HOME=C:\hadoop

REM Create output/log folders if not exist
if not exist "E:\01. Python Basic\cricket_stream\executed" mkdir "E:\01. Python Basic\cricket_stream\executed"
if not exist "E:\01. Python Basic\cricket_stream\logs" mkdir "E:\01. Python Basic\cricket_stream\logs"

REM Run notebook with timestamped output and log
papermill "E:\01. Python Basic\cricket_stream\my_spark_job.ipynb" "E:\01. Python Basic\cricket_stream\executed\output_%DATE:/=%%TIME::=%.ipynb" >> "E:\01. Python Basic\cricket_stream\logs\log%DATE:/=%%TIME::=_%.txt" 2>&1