import pandas as pd
from sqlalchemy import create_engine, text

# Setup database connection
engine = create_engine("mysql+pymysql://root:Abbas123#@localhost/healthcare_dwh")
conn = engine.connect()

# 1️⃣ Average treatment cost per diagnosis by age group and gender
query1 = """
SELECT 
    d.diagnosis_code,
    p.age,
    p.gender,
    AVG(f.procedure_cost + f.medication_cost) AS avg_treatment_cost
FROM 
    Fact_TreatmentCosts f
JOIN Patient_Dim p ON f.patient_id = p.patient_id
JOIN Diagnosis_Dim d ON f.diagnosis_code = d.diagnosis_code
GROUP BY 
    d.diagnosis_code, p.age, p.gender;
"""

# Query 2: Medication stock levels and replenishment alerts
query2 = """
SELECT 
    m.medication_name,
    s.supplier_name,
    f.stock_level,
    f.dispensed,
    CASE 
        WHEN f.stock_level < (f.dispensed * 2) THEN 'REPLENISH'
        ELSE 'OK'
    END AS alert_status
FROM 
    Fact_MedicationDemand f
JOIN Medication_Dim m ON f.medication_id = m.medication_id
JOIN Supplier_Dim s ON f.supplier_id = s.supplier_id;
"""

# Query 3: Correlation between disease outbreaks and hospital visits
query3 = """
SELECT 
    z.zip_code,
    d.diagnosis_code,
    SUM(f.visit_count) AS total_visits,
    SUM(f.flu_cases) AS total_flu_cases,
    SUM(f.covid_cases) AS total_covid_cases
FROM 
    Fact_OutbreakCorrelation f
JOIN Diagnosis_Dim d ON f.diagnosis_code = d.diagnosis_code
JOIN ZipCode_Dim z ON f.zip_code = z.zip_code
GROUP BY 
    z.zip_code, d.diagnosis_code;

"""

print("\n✅ OLAP queries executed and reports generated successfully!")

while True:
    choice = int(input('''Enter Choice :
                   1.disease_outbreak
                   2.treatment_cost
                   3.med_stock
                   4.exit
                   '''))
    if choice == 1:
       print("\n🔍 Disease outbreaks vs hospital visit spikes over time:")
       df3 = pd.read_sql(text(query3), conn)
       print(df3)
    elif choice == 2:
        print("\n🔍 Average treatment cost per diagnosis by age group and gender:")
        df1 = pd.read_sql(text(query1), conn)
        print(df1)

    elif choice == 3: 
        print("\n🔍 Medication stock levels and replenishment alerts based on patient demand trends:")
        df2 = pd.read_sql(text(query2), conn)
        print(df2)

    elif choice == 4:
        break
    else:
        print("Invalid Choice")

# Optional: Save reports
# df1.to_csv("avg_treatment_cost_report.csv", index=False)
# df2.to_csv("stock_replenishment_alerts.csv", index=False)
# df3.to_csv("outbreak_vs_visits.csv", index=False)
