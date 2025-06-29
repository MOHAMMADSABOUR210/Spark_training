@echo off

REM 
call D:\Programming\Data_Engineering\Apache_Spark\spark-env\Scripts\activate.bat

REM 
set PYSPARK_PYTHON=D:\Programming\Data_Engineering\Apache_Spark\spark-env\Scripts\python.exe
set PYSPARK_DRIVER_PYTHON=D:\Programming\Data_Engineering\Apache_Spark\spark-env\Scripts\python.exe

REM 
%PYSPARK_PYTHON% create_rdd.py

pause
