"""
  Employee Analytics Pipeline - using Pandas

    EXTRACT  -> read raw CSV 
    TRANSFORM -> clean , add Columns , aggregate 
    LOAD -> Write processes CSV + print report 

    Run :- python etl_pipeline.py
"""

import os
import pandas as pd

# print("hello ajay and alok how are  you ?")
RAW_FILE = "input/employee_data2.csv" # input file address
OUTPUT_DIR = "output"       # output file directory
OUTPUT_FILE = os.path.join(OUTPUT_DIR,"employee_report1.csv")  # where we want to store the report file 

# ========================================================================
# EXTRACT 
# ========================================================================
# pd.DataFrame -> Loaded data in the tabular format.
# (path :str) -> File path to the csv file 

def extract(path:str) -> pd.DataFrame:
    """
     Read the raw CSV file into a Pandas Dataframe
    """
    print(f"[EXTRACT] Reading {path} .......")
    df = pd.read_csv(path)
    print(f"[EXTRACT] Loaded {df.shape[0]} rows , {df.shape[1]} columns")
    return df



# ========================================================================
# EXTRACT 
# ========================================================================

def clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handle missing values (real-world data is rarely perfact)
    """
    print("[TRANSFORM] cleaning data......")

    before = df.isnull().sum().sum()

    # fill missing department/city/Name with 'Unknown' ,
    for col in ["Department","City","Name"]:
        if col in df.columns:
            df[col] = df[col].fillna("Unknown")

    for col in ["Age","Salary","Experience"]:
        if col in df.columns:
            df[col] = df[col].fillna(df[col].mean())

    after =  df.isnull().sum().sum()
    print(f"[TRANSFORM] Missing Values fixed : {before} -> {after}")
    return df


def enrich(df: pd.DataFrame) -> pd.DataFrame:
    """ Add Derived Column """
    print("[TRANFORM] Adding derived columns.....")
    df["Bonus"] = df["Salary"]*0.10
    df["TotalPay"] = df["Salary"]+df["Bonus"]
    df["ExperienceLevel"] = df["Experience"].apply(
        lambda x :"Senior" if x>=5 else ("Mid" if x>=3 else "Junior")
    )
    return df

def transform(df: pd.DataFrame) -> pd.DataFrame:
    df=clean(df)
    df=enrich(df)
    return df

# ===============================================================
# Load 
# ===============================================================

def load(df: pd.DataFrame,output_path: str)-> None:
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False) # Prevents pandas from writing the df index as an extra column
    print(f"[Load] Report written to {output_path}")

# ========================================================================
# Anaylsis/ Report  (what the dashboard" would consume)
# ========================================================================

def report(df: pd.DataFrame) -> None:
    print("\n ==== EMPLOYEE ANALYTICS REPORT ====")
    print(f"Total Employee   :  {len(df)}")
    print(f"Highest Salary :  {df['Salary'].max()} ({df.loc[df['Salary'].idxmax(),'Name']})") # df['salary'].idxmax():- find the index/position of the employee with the highest salary
    print(f"Lowest Salary : {df['Salary'].min()} ({df.loc[df['Salary'].idxmin(),'Name']})")
    print(f"Average Experience : {df['Experience'].mean():.2f} years") # 2f -> Format the result to exactly 2 decimal places

    print("\n Department-wise average Salary:")
    print(df.groupby("Department")['Salary'].mean().round(2))

    print("\nCity-wise Employee count :")
    print(df.groupby("City")["EmployeeID"].count())
    print("=========================================================")


def run_pipeline():
    df = extract(RAW_FILE)
    df = transform(df)
    load(df,OUTPUT_FILE)
    report(df)

run_pipeline()
