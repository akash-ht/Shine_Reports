SELECT A.jobid AS Job_Id,REPLACE(REPLACE(REPLACE(A.displayname,'\n',''),'\t',''),',','') AS CompanyName,A.jobcreationdate AS JobCreationDate,
companyid_id AS AccountId,REPLACE(REPLACE(REPLACE(B.company_name,'\n',''),'\t',''),',','')  AS AccountName,
case account_type
when 0 THEN "Company"
when 1 THEN "Consultant"
when 2 THEN "Others"
when 3 THEN "Enterprise"
ELSE "Not Specified"
END AS account_type,jobcreationdate AS ActivationDate, 
expirydate AS ExpirationDate, IF(A.isbocreated, D.username, d.username) AS UserName, 
IF(A.isbocreated, "Recruiter Servicing", "Client") AS PublisherType, C.MatchedApplications, C.Applications, C.FBApplications, K.sub_field_enu AS Sub_FA, 
K.field_enu AS FA, L.display AS Min_Experience, M.display AS Max_Experience, G.industry_desc AS Industry,
publisheddate AS PublishedDate, A.lastrematched AS lastrematched, I.city_desc As Location, REPLACE(A.jobtitle,',',';'), A.isbocreated,
N.text_value AS Min_Salary, O.text_value AS Max_Salary,/* IF(TotalSalePriceAllSales = 0, 'Free', 'Paid') AS "Free/Paid",*/
IF(XY.enabled = 1 AND XY.price != 0 AND XY.expiry_date > CURDATE(),'Paid','Free') AS 'Free/Paid',republisheddate AS RepublishedDate
-- , REPLACE(REPLACE(REPLACE(A.description,'\n',''),',',''),'\t','') AS Description-- , Paid/Non-Paid
FROM ShineReport.LiveJobsApplications C
LEFT JOIN SumoPlus.recruiter_job A
ON C.JobId = A.jobid
LEFT JOIN SumoPlus.backoffice_companyaccount B
ON A.companyid_id = B.id
LEFT JOIN SumoPlus.auth_user D
ON A.createdby = D.id
LEFT JOIN SumoPlus.recruiter_recruiter d
ON A.createdby = d.id
LEFT JOIN SumoPlus.lookup_industry G
ON A.industry = G.industry_id
LEFT JOIN SumoPlus.recruiter_jobattribute H
ON A.jobid = H.jobid_id AND H.AttType = 13
LEFT JOIN SumoPlus.lookup_city_distinct I
ON H.AttValue = I.city_id
LEFT JOIN SumoPlus.recruiter_jobattribute J
ON A.jobid = J.jobid_id AND J.AttType = 12
LEFT JOIN SumoPlus.lookup_subfunctionalarea K
ON J.AttValue = K.sub_field_id
LEFT JOIN SumoPlus.lookup_experience L
ON L.value = A.minexperience
LEFT JOIN SumoPlus.lookup_experience M
ON M.value = A.maxexperience
LEFT JOIN SumoPlus.lookup_annualsalary N
ON A.salarymin= N.salary_id
LEFT JOIN SumoPlus.lookup_annualsalary O
ON A.salarymax = O.salary_id
LEFT JOIN ShineReport.AccountType P
ON A.companyid_id = P.company_account_id
LEFT JOIN 
(SELECT company_account_id,SUM(final_sale_price)as price,enabled,MAX(expiry_date)as expiry_date 
	from SumoPlus.backoffice_accountsales a1 
	where enabled in 
	(select min(enabled) from SumoPlus.backoffice_accountsales where a1.company_account_id=company_account_id)
	group by 1) AS XY
ON B.id = XY.company_account_id
GROUP BY 1
