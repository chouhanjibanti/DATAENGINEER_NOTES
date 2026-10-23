"""
  Employee Analytics Pipeline - using Pyspark 

     EXTRACT  -> spark.read.csv()
      TRANSFORM -> clean , add Columns , window functions  
      LOAD -> Write parquet + CSV processes  print report 

      RUN :- pip install pyspark 
      python pyspark_etl_pipeline.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# configuration constants (same as pandas)
RAW_FILE = "input/employee_data1.csv"
OUTPUT_DIR = "output/employee_spark_report"

def get_spark():
    """ create and configure a spark session """
    return (
        SparkSession.builder.appName("EmployeeeAnalyticsPipleline").master("local[*]").getOrCreate()
    )

# ========================================================================
# PHASE 1: EXTRACT [using pyspark]
# ========================================================================

def extract(spark,path:str):
    """ Read the raw csv file into a spark dataframe """

    print(f"[EXTRACT]  Reading {path} ....")
    df = spark.read.csv(path , header=True , inferSchema=True)
    print(f"[EXTRACT] Loaded {df.count()}  rows , {len(df.columns)} columns")
    return df

# ========================================================================
# PHASE 2: TRANSFORM [using pyspark] - clean , enrich 
# ========================================================================

def clean(df):
    """ Handles missing values using pysprak operation """

    print("[TRANSFORM] Cleaning Data.....")

    # fill categorical columns with "Unknown"
    # Dictionary Syntax : {column_name:fill_value}

    df = df.fillna({"Department":"Unknown","City":"Unknown","Name":"Unknown"})

    # Fill numeric columns with mean (more complex in spark as compare to pandas)
    for col_name in ["Age","Salary","Experience"]:
        # calculate mean for the column # F.mean() is a spark aggregate function 
        mean_val= df.select(F.mean(F.col(col_name))).first()[0]
        df = df.fillna({col_name:mean_val})
    return df


def enrich(df):
    """ Add derived columns using pyspark including window functions"""

    print("[TRANSFORM] Adding derived columns + window function .....")

    df = df.withColumn("Bonus", F.col("Salary") * 0.10) 

    df = df.withColumn("TotalPay", F.col("Salary")+ F.col("Bonus"))

    df=df.withColumn("ExperienceLevel",F.when(F.col("Experience") >= 5 , "Senior")
                     .when(F.col("Experience") >= 3 , "Mid")
                     .otherwise("Junior"))

  # Window function :- rank employee by salary within each department
    dept_window = Window.partitionBy("Department").orderBy(F.col("Salary").desc())
    df=df.withColumn("SalaryRankInDept", F.rank().over(dept_window))
    return df


def transform(df):
    df = clean(df)
    df = enrich(df)
    return df


# ========================================================================
# PHASE 3: Load [using pyspark]  
# ========================================================================

def load(df,output_dir:str):
    os.makedirs(output_dir, exist_ok=True)
    parquet_path = os.path.join(output_dir,"parquet")
    csv_path = os.path.join(output_dir,"csv")
    df.write.mode("overwrite").parquet(parquet_path)
    df.write.mode("overwrite").option("header",True).csv(csv_path)
    print(f"[Load] Report written to {parquet_path} and {csv_path}")


# ========================================================================
#  Analytics (Sparlk SQL) 
# ========================================================================

def report(spark,df):
    df.createOrReplaceTempView("employee")

    print("\n ======= EMPLOYEE ANALYTICS REPORT (SPARk SQL) ========")

    print(" Highest Paid Employee per department : ")
    spark.sql(
        """
         SELECT Department , Name , Salary
         FROM EMPLOYEE
         WHERE SalaryRankInDept =1
         ORDER BY Department
        """
    ).show()

    print(" Department-wise average Salary : ")
    spark.sql(
        """
         SELECT Department , ROUND(AVG(Salary),2) AS AvgSalary
         FROM Employee
         GROUP BY Department 
         ORDER BY AvgSalary DESC
        """
    ).show()

    print("City-Wise Employee Count : ")
    spark.sql(
        """ 
        SELECT City , COUNT(*) AS EmployeeCount
        FROM EMPLOYEE
        GROUP BY City
        ORDER BY EmployeeCount DESC   
        """
    ).show()

    print("=====================================================")

# ========================================================================
# PIPELINE ENTRYPOINT
# ========================================================================

def run_pipeline():
    spark=get_spark()
    df=extract(spark,RAW_FILE)
    df=transform(df)
    load(df,OUTPUT_DIR)
    report(spark,df)
    spark.stop()

run_pipeline()





