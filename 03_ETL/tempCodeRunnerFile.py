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
