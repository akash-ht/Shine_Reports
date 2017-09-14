SELECT Username, FirstName, CellPhone, TotalExperience
FROM INDExportDB.CandidatesInc
WHERE DATE(RegistrationDate) =  DATE_SUB(CURDATE(), INTERVAL 1 DAY)
AND Status='Activated'
LIMIT 600;
