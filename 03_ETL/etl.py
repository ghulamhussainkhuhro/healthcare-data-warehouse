import pandas as pd
import pymysql
from sqlalchemy import create_engine,text
from datetime import datetime

# MySQL connection
engine = create_engine("mysql+pymysql://root:Abbas123#@localhost/healthcare_dwh", isolation_level="AUTOCOMMIT")
conn = engine.connect()

# Load CSV files
patient_records = "D:/DWH CEP/DataSources/patient_records.csv"
billing_system = "D:/DWH CEP/DataSources/billing_system.csv"
pharmacy_inventory = "D:/DWH CEP/DataSources/pharmacy_inventory.csv"
public_health_data = "D:/DWH CEP/DataSources/public_health_data.csv"

patients_df = pd.read_csv(patient_records,encoding='utf-8')
billing_df = pd.read_csv(billing_system)
pharmacy_df = pd.read_csv(pharmacy_inventory)
public_health_df = pd.read_csv(public_health_data)

# Clean and standardize date fields to YYYY-MM-DD
def clean_date_column(df, column_name):
    df[column_name] = pd.to_datetime(df[column_name], errors='coerce').dt.date
    df.dropna(subset=[column_name], inplace=True)

# Clean patient records
clean_date_column(patients_df, 'Visit_Date')
patients_df.drop_duplicates(subset='Patient_ID', inplace=True)

# Clean billing system
clean_date_column(billing_df, 'Billing_Date')
billing_df.drop_duplicates(subset='Patient_ID', inplace=True)

# Clean pharmacy inventory
pharmacy_df.dropna(inplace=True)
pharmacy_df.drop_duplicates(subset='Medicine_Name', inplace=True)

# Clean public health data
clean_date_column(public_health_df, 'Report_Date')


# Start loading to DB
with engine.begin() as conn:
    # dim_patient
    for _, row in patients_df.iterrows():
        conn.execute(text("""
            INSERT INTO Patient_Dim (patient_id, name, age, gender)
            VALUES (:patient_id, :name, :age, :gender)
            ON DUPLICATE KEY UPDATE name=VALUES(name), age=VALUES(age), gender=VALUES(gender)
        """), {
            "patient_id": row['Patient_ID'],
            "name": row['Name'],
            "age": row['Age'],
            "gender": row['Gender']
        })
    for _, row in patients_df.iterrows():
        conn.execute(text("""
            INSERT IGNORE INTO Diagnosis_Dim (diagnosis_code, diagnosis_description)
            VALUES (:diagnosis_code, :diagnosis_description)
            ON DUPLICATE KEY UPDATE diagnosis_code=VALUES(diagnosis_code), diagnosis_description=VALUES(diagnosis_description)
        """), {
            "diagnosis_code": row['Diagnosis'],
            "diagnosis_description": row['Diagnosis_Description'],
        })

    # dim_date
    all_dates = pd.concat([
        patients_df['Visit_Date'],
        billing_df['Billing_Date'],
        public_health_df['Report_Date']
    ]).dropna().drop_duplicates()

    for date in all_dates:
        conn.execute(text("""
            INSERT IGNORE INTO Date_Dim (full_date, day, month, year,day_of_week)
            VALUES (:full_date, DAY(:full_date), MONTH(:full_date), YEAR(:full_date),DAYNAME(:full_date))
        """), {"full_date": date})
    # dim_supplier
    for _, row in pharmacy_df.iterrows():
        conn.execute(text("""
            INSERT IGNORE INTO Supplier_Dim (supplier_id,supplier_name)
            VALUES (:supplier_id,:supplier_name)
        """), {"supplier_id": row['Supplier_ID'],
              "supplier_name" :row['Supplier_Name']
        })

    for _, row in pharmacy_df.iterrows():
        conn.execute(text("""
            INSERT IGNORE INTO Medication_Dim (medication_name)
            VALUES (:medication_name)
        """), {"medication_name": row['Medicine_Name']
        })
   

    for _, row in public_health_df.iterrows():
            conn.execute(text("""
                INSERT INTO ZipCode_Dim (zip_code, city , region)
                VALUES (:zip_code, :city, :region)
                ON DUPLICATE KEY UPDATE
                zip_code = VALUES(zip_code),
                city = VALUES(city),
                region = VALUES(region)
                 """), {
                 "zip_code": int(row['Zip_Code']),
                "city": row['City'],
                "region": row['Region'],
        })
        

        # FACT: Treatment Costs
    for _, row in billing_df.iterrows():
        visit_match = patients_df[patients_df['Patient_ID'] == row['Patient_ID']]
        if not visit_match.empty:
            diagnosis_code = visit_match.iloc[0]['Diagnosis']
            visit_date = visit_match.iloc[0]['Visit_Date']
            date_result = conn.execute(text("SELECT date_id FROM Date_Dim WHERE full_date = :date"),
                                       {"date": visit_date}).fetchone()

            if date_result:
                conn.execute(text("""
                    INSERT INTO Fact_TreatmentCosts (patient_id, diagnosis_code, date_id, procedure_cost, medication_cost, insurance_claim)
                    VALUES (:patient_id, :diagnosis_code, :date_id, :procedure_cost, :medication_cost, :insurance_claim)
                """), {
                    "patient_id": row['Patient_ID'],
                    "diagnosis_code": diagnosis_code,
                    "date_id": date_result[0],
                    "procedure_cost": row['Procedure_Cost'],
                    "medication_cost": row['Medication_Cost'],
                    "insurance_claim": bool(row['Insurance_Claim'])
                })

    # FACT: Medication Demand
    for _, row in pharmacy_df.iterrows():
        med_result = conn.execute(text("SELECT medication_id FROM Medication_Dim WHERE medication_name = :name"),
                                  {"name": row['Medicine_Name']}).fetchone()

        default_date = '2025-01-01'  # You can change this to any default date
        date_id = conn.execute(text("SELECT date_id FROM Date_Dim WHERE full_date = :default_date"),
                               {"default_date": default_date}).fetchone()

        if med_result and date_id:
            conn.execute(text("""
                INSERT INTO Fact_MedicationDemand (medication_id, supplier_id, date_id, dispensed, stock_level)
                VALUES (:medication_id, :supplier_id, :date_id, :dispensed, :stock_level)
            """), {
                "medication_id": med_result[0],
                "supplier_id": row['Supplier_ID'],
                "date_id": date_id[0],
                "dispensed": row['Dispensed'],
                "stock_level": row['Stock_Level']
            })

    # FACT: Outbreak Correlation
    grouped_visits = patients_df.groupby([patients_df['Diagnosis'], patients_df['Visit_Date']]).size().reset_index(name='visit_count')
    for _, outbreak in public_health_df.iterrows():
        zip_code = int(outbreak['Zip_Code'])
        report_date = outbreak['Report_Date']
        date_result = conn.execute(text("SELECT date_id FROM Date_Dim WHERE full_date = :date"),
                                   {"date": report_date}).fetchone()

        for _, row in grouped_visits.iterrows():
            diagnosis_code = row['Diagnosis']
            visit_date = row['Visit_Date']
            if visit_date == report_date and date_result:
                conn.execute(text("""
                    INSERT INTO Fact_OutbreakCorrelation (zip_code, diagnosis_code, date_id, visit_count, flu_cases, covid_cases)
                    VALUES (:zip_code, :diagnosis_code, :date_id, :visit_count, :flu_cases, :covid_cases)
                """), {
                    "zip_code": zip_code,
                    "diagnosis_code": diagnosis_code,
                    "date_id": date_result[0],
                    "visit_count": row['visit_count'],
                    "flu_cases": outbreak['Flu_Cases'],
                    "covid_cases": outbreak['COVID_Cases']
                })

print("✅ ETL process completed successfully!")
