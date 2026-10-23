# print("hello")

"""
 Emmployee Data Analytics Pipeline using pandas
 =============================================

 EXTRACT -> read raw csv data from file system
 TRANSFORM -> clean data , and derived columns , perform aggregations
 LOAD -> write processed data to CSV + generated analtics Report 
"""

import os
import pandas as pd

RAW_FILE = "input/employee_data1.csv"
OUTPUT_DIR = "output"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "employee_report.csv")


#===========================================================
# EXTRACT
#===========================================================

def extract(path:str) -> pd.DataFrame:
    """Read the raw csv into a dataframe"""
    print(f"[EXTRACT] Reading {path}....")
    df = pd.read_csv(path)
    print(f"[EXTRACT] Loaded {df.shape[0]} rows , {df.shape[1]} columns")
    return df


#===========================================================
# TRANSFORM
#===========================================================

def clean(df: pd.DataFrame) -> pd.DataFrame:
    """
     Handling Missing values
    """
    print("[TRANSFORM] Cleaning data.....")

    before = df.isnull.sum().sum()

    # fill missing department/city and name with 'unknown' , numeric gaps with column mean
    for col in ["Department","City","Name"]:
        if col in df.columns:
            df[col] = df[col].fillna("unknown")

    for col in ["Age","Salary","Experience"]:
        if col in df.columns:
            df[col] = df[col].fillna(df[col].mean())

    after = df.isnull.sum().sum()
    print(f"[TRANSFORM] missing values fixed : {before} -> {after}")
    return df


def enrich(df: pd.DataFrame) -> pd.DataFrame:
    """ Add derived columns """
    print("[TRANSFORM] Adding derived columns")
    df["Bonus"] = df["Salary"] * 0.10
    df["TotalPay"] = df["Salary"] + df["Bonus"]
    df["ExperienceLevel"] = df["Experience"].apply(
        lambda x: "Senior" if x >=5 else ("Mid" if x >=3 else "Junior")
    )
    return df


def transform(df:pd.DataFrame) -> pd.DataFrame:
    df=clean(df)
    df=enrich(df)
    return df 

#===========================================================
# LOAD
#===========================================================

def load(df:pd.DataFrame , output_path:str) -> None:
    """ Write the processed data to disk """
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path,index=False)
    print(f"[LOAD] Report written to {output_path}")


#===========================================================
# ANALYSIS /REPORT 
#===========================================================



