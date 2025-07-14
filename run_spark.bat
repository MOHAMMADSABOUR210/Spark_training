@echo off

REM 
call D:\Programming\Data_Engineering\Apache_Spark\spark-env\Scripts\activate.bat

REM 
set PYSPARK_PYTHON=D:\Programming\Data_Engineering\Apache_Spark\spark-env\Scripts\python.exe
set PYSPARK_DRIVER_PYTHON=D:\Programming\Data_Engineering\Apache_Spark\spark-env\Scripts\python.exe

REM 
@REM %PYSPARK_PYTHON% create_rdd.py
@REM %PYSPARK_PYTHON% test_pyspark.py
@REM %PYSPARK_PYTHON% word_count.py
@REM %PYSPARK_PYTHON% rdd_op.py
@REM %PYSPARK_PYTHON% rdd_cv.py
@REM %PYSPARK_PYTHON% spark_Collection.py
@REM %PYSPARK_PYTHON% CartesianM.py
%PYSPARK_PYTHON% ReduceSpark.py


pause
