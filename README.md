# Healthcare Data Warehouse Project

This repository contains the **Healthcare Data Warehouse** project developed as part of the **Data Warehousing (SW228)** course at **Mehran University of Engineering and Technology, Jamshoro**.

## 📋 Project Overview

The goal of this project was to build a fully functional data warehouse for a regional healthcare provider. The system collects and integrates data from multiple sources to optimize patient care analysis, resource allocation, and operational efficiency.

### Key Features:
- **Data Sources Integrated**:
  - Patient Records System
  - Hospital Billing System
  - Pharmacy Inventory System
  - External Public Health Data
- **Design Models**:
  - Conceptual Design
  - Logical Design
  - Physical Design
- **Schema Type**:
  - Fact Constellation Schema (also known as Galaxy Schema)
- **Technologies Used**:
  - Python (for ETL processes)
  - MySQL (for data storage)
  - SQL (for business queries and insights)

---

## ⚙️ ETL Process

- **Extraction**: Connected multiple data sources using Python scripts.
- **Transformation**: Handled inconsistencies such as:
  - Inconsistent date formats across sources.
  - Missing values in billing system (~10% missing).
- **Loading**: Loaded clean and transformed data into MySQL tables.

### Data Quality Issues and Solutions:
- **Missing Values**: Imputed missing billing values with either average procedure cost or used domain-specific logic.
- **Date Format Inconsistencies**: Standardized all dates to `YYYY-MM-DD` format during transformation stage.

---

## 🗄️ Data Warehouse Schema

### Fact Tables:
- `FactPatientVisits`
- `FactBilling`
- `FactPharmacyDispensing`

### Dimension Tables:
- `DimPatient`
- `DimDiagnosis`
- `DimTreatment`
- `DimMedication`
- `DimDate`

### Special Handling:
- **Slowly Changing Dimension (SCD)**:
  - Implemented for `DimPatient` to handle address changes over time (Type 2 SCD).

### Schema Diagram:

> [Insert star/snowflake/fact constellation diagram image here]

---

## 🚀 Performance Optimization

To optimize query performance:
- **Indexing**: Added indexes on commonly queried columns (e.g., `patient_id`, `diagnosis_code`, `date_id`).
- **Partitioning**: Partitioned large fact tables by date to improve query performance on time-based analytics.

---

## 📈 Scalability Considerations

- **Horizontal Scaling**:
  - Proposed sharding the database by hospital location to handle a 10x increase in data volume.
  - This ensures faster query performance and more manageable data loads as the number of hospitals expands to 50.

---

## 🔎 Business Insights Generated

- Average treatment cost per diagnosis categorized by age group and gender.
- Medication stock levels and replenishment alerts based on patient demand trends.
- Correlation analysis between local disease outbreaks and hospital visit spikes over time.

---

## 📚 Research Work

- Surveyed different data warehouse architectures (Inmon, Kimball, Data Vault).
- Evaluated fact constellation schema as best fit for multiple related fact tables.
- Analyzed pros and cons of physical schema designs and ETL strategies.

---

## 🛠️ How to Run the Project

1. Clone this repository.
2. Set up MySQL database using provided schema files.
3. Execute Python ETL scripts to populate the database.
4. Run the provided SQL queries to analyze the data.

---

## 📜 Assumptions

- Daily addition of 10,000 patient records and 5,000 billing transactions.
- Public health data updated monthly.
- Average concurrent user access assumed to be around 20 analysts during peak hours.

---

## 📃 License

This project is developed for educational purposes.

---

## 🤝 Acknowledgement

Course: **Data Warehousing (SW228)**  
Instructor: **Mr. Naeem Ahmed**  
Department of Software Engineering, MUET Jamshoro

