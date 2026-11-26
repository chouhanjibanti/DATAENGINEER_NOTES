"""
EMPLOYEE DATA ANALYTICS PIPELINE 

EXTRACT -> spark.read.csv()
TRANSFORM -> clean , add columns , window functions 
LOAD -> write Parquet + csv , run spark sql report 

Run : pip install pyspark 
     python pyspark_etl.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Configuration 
RAW_FILE = "input/employee_data1.csv"
OUTPUT_DIR= "output/employee_report_spark"

def get_spark():

    """ create and configure a spark session"""

    return (
        SparkSession.builder.
        appName("EmployeeAnalyticsPipeline")
        .master("local[*]")  # use all local cpu cores 
        .getOrCreate()
    )


# -----------------------------------------------------------
# EXTRACT
# ----------------------------------------------------------

def extract(spark,path:str):
    print(f"[EXTRACT] Reading {path}....")
    df = spark.read.csv(path , header=True,inferSchema=True)
    print(f"[EXTRACT] Loaded {df.count()} rows , {len(df.columns)} columns")
    return df


# -----------------------------------------------------------
# TRANSFORM
# ----------------------------------------------------------

def clean(df):
     print("[TRANSFORM] Cleaning Data.....")
     df=df.fillna({"Department":"unknown","City":"Unknown","Name":"Unknown"})
     for col_name in ["Age","Salary","Experience"]:
        mean_val = df.select(F.mean(F.col(col_name))).first()[0]
        df=df.fillna({col_name:mean_val})
     return df

def enrich(df):
    print("[TRANSFORM] Adding derived columns + window functions....")
    df=df.withColumn("Bonus", F.col("Salary") * 0.10)
    df=df.withColumn("TotalPay", F.col("Salary") + F.col("Bonus"))
    df=df.withColumn(
        "ExperienceLevel",
        F.when(F.col("Experience") >=5 , "Senior")
        .when(F.col("Experience") >=3 ,"Mid") 
        .otherwise("Junior") 
     )

    # Window function :- rank employees by salary within each department
    dept_window=Window.partitionBy("Department").orderBy(F.col("Salary").desc())
    df=df.withColumn("SalaryRankInDept", F.rank().over(dept_window))
    return df


def transform(df):
    df=clean(df)
    df=enrich(df)
    return df 

# -----------------------------------------------------------
# LOAD
# -----------------------------------------------------------

def load(df,output_dir:str):
    os.makedirs(output_dir,exist_ok=True)
    parquet_path = os.path.join(output_dir,"parquet")
    csv_path = os.path.join(output_dir,"csv")
    df.write.mode("overwrite").parquet(parquet_path)
    df.write.mode("overwrite").option("header",True).csv(csv_path)
    print(f"[LOAD] Report written to {parquet_path} and {csv_path}")

# -----------------------------------------------------------
# ANALYSIS (Spark SQL)
# ----------------------------------------------------------

def report(spark,df):
    df.createOrReplaceTempView("employee")

    print("\n ==== EMPLOYEE ANALYTICS REPORT (Spark SQL ) ==== ")

    print("Highest Paid employee per department")
    spark.sql(
        """ 
          SELECT Department,Name,Salary
          FROM employee
          WHERE SalaryRankInDept = 1
          ORDER by Department
        """
    ).show()

    print("Department wise Average Salary :")
    spark.sql(
        """
        SELECT Department , ROUND(AVG(Salary),2) as AvgSalary
        FROM employee
        GROUP BY Deparment 
        ORDER BY AvgSalary DESC
        """
    ).show()

    print("City wise Employee count :")
    spark.sql(
        """
           SELECT City , COUNT(*) as EmployeeCount
           FROM employee
           GROUP BY City
           ORDER BY EmployeeCount DESC
        """
    ).show()

    print("==========================================")


#===========================================================
# PIPELINE ENTRYPOINT
#===========================================================

def run_pipeline():
    spark=get_spark()
    df=extract(spark,RAW_FILE)
    df=transform(df)
    load(df,OUTPUT_DIR)
    report(spark,df)
    spark.stop()


run_pipeline()