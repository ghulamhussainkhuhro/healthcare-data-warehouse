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