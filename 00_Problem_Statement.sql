Create database healthcare_dwh;
drop database healthcare_dwh;
use healthcare_dwh;


CREATE TABLE Patient_Dim (
    patient_id INT PRIMARY KEY,
    name VARCHAR(100),
    age INT,
    gender VARCHAR(10)
);

CREATE TABLE Diagnosis_Dim (
    diagnosis_code VARCHAR(10) PRIMARY KEY,
    diagnosis_description VARCHAR(255)
);

CREATE TABLE Date_Dim (
    date_id INT AUTO_INCREMENT PRIMARY KEY,
    full_date DATE,
    day INT,
    month INT,
    year INT,
    day_of_week VARCHAR(10)
);

CREATE TABLE Medication_Dim (
    medication_id INT AUTO_INCREMENT PRIMARY KEY,
    medication_name VARCHAR(100) unique
);
drop TABLE Medication_Dim;

CREATE TABLE Supplier_Dim (
    supplier_id INT PRIMARY KEY,
    supplier_name VARCHAR(100)
);

CREATE TABLE ZipCode_Dim (
    zip_code VARCHAR(10) PRIMARY KEY,
    city VARCHAR(100),
    region VARCHAR(100)
);


CREATE TABLE Fact_OutbreakCorrelation (
    correlation_id INT AUTO_INCREMENT PRIMARY KEY,
    zip_code VARCHAR(10),
    diagnosis_code VARCHAR(10),
    date_id INT,
    visit_count INT,
    flu_cases INT,
    covid_cases INT,
    FOREIGN KEY (zip_code) REFERENCES ZipCode_Dim(zip_code),
    FOREIGN KEY (diagnosis_code) REFERENCES Diagnosis_Dim(diagnosis_code),
    FOREIGN KEY (date_id) REFERENCES Date_Dim(date_id)
);
drop TABLE Fact_OutbreakCorrelation;
CREATE TABLE Fact_MedicationDemand (
    demand_id INT AUTO_INCREMENT PRIMARY KEY,
    medication_id INT,
    supplier_id INT,
    date_id INT,
    dispensed INT,
    stock_level INT,
    FOREIGN KEY (medication_id) REFERENCES Medication_Dim(medication_id),
    FOREIGN KEY (supplier_id) REFERENCES Supplier_Dim(supplier_id),
    FOREIGN KEY (date_id) REFERENCES Date_Dim(date_id)
);
drop TABLE Fact_MedicationDemand;

CREATE TABLE Fact_TreatmentCosts (
    treatment_id INT AUTO_INCREMENT PRIMARY KEY,
    patient_id INT,
    diagnosis_code VARCHAR(10),
    date_id INT,
    procedure_cost DECIMAL(10,2),
    medication_cost DECIMAL(10,2),
    insurance_claim BOOLEAN,
    FOREIGN KEY (patient_id) REFERENCES Patient_Dim(patient_id),
    FOREIGN KEY (diagnosis_code) REFERENCES Diagnosis_Dim(diagnosis_code),
    FOREIGN KEY (date_id) REFERENCES Date_Dim(date_id)
);

-- Select all data from Patient_Dim
SELECT * FROM Patient_Dim;

-- Select all data from Diagnosis_Dim
SELECT * FROM Diagnosis_Dim;

-- Select all data from Date_Dim
SELECT * FROM Date_Dim;

-- Select all data from Medication_Dim
SELECT * FROM Medication_Dim;

-- Select all data from Supplier_Dim
SELECT * FROM Supplier_Dim;

-- Select all data from ZipCode_Dim
SELECT * FROM ZipCode_Dim;

-- Select all data from Fact_OutbreakCorrelation
SELECT * FROM Fact_OutbreakCorrelation;

-- Select all data from Fact_MedicationDemand
SELECT * FROM Fact_MedicationDemand;

-- Select all data from Fact_TreatmentCosts
SELECT * FROM Fact_TreatmentCosts;

