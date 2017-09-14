SELECT A.id AS 'AccountId', company_name AS 'AccountName', 1 AS 'CountryID', D.city_desc AS 'City', address AS 'Address', 
'' AS 'ZipCode', legal_type AS 'CompanyStatusId', '' AS 'ExternalAccountID',/* creation_mode AS 'IsOnlineAccount', */
B.id AS 'AccountContactID', B.first_name AS 'AccountContactName', '' AS 'AccountContactTitle', B.email AS 'AccountContactEmail', 
B.contact_number AS 'AccountContactPhone', contact_name AS 'AccountFinanceName', contact_title AS 'AccountFinanceTitle', 
contact_email AS 'AccountFinanceEmail', A.contact_number AS 'AccountFinancePhone', '' AS 'AccountFinancePhone1', 
/*sales_manager_id AS 'SalesManagerId', C.first_name AS 'SalesManagerName', */taxable AS 'enableTaxCalculation', 
description AS 'CompanyDescription', website AS 'WebSite', '' AS 'IndustryId', Count(B.id) AS 'NumberofEmployeesID', 
account_type AS 'AccountTypeID', '' AS 'TurnOveroftheCompanyID'
FROM backoffice_companyaccount A
LEFT JOIN recruiter_recruiter B
ON A.id = B.company_id
LEFT JOIN auth_user C
ON A.sales_manager_id = C.id
LEFT JOIN SumoPlus_Complement.lookup_city D
ON A.location = D.city_id
-- WHERE created_date BETWEEN '2011-06-20' AND '2011-07-17'
-- Where A.id IN (52800, 52801, 52802, 52803, 52804, 52805, 52826)
-- WHERE DATE(A.created_date) = DATE_ADD(CURDATE(), INTERVAL -1 DAY)
AND IsPrimary = 1
GROUP BY company_id;
