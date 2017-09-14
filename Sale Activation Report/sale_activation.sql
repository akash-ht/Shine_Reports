SELECT REPLACE(B.name, ',', ';') AS 'Package Name', DATE_FORMAT(A.created_date, "%d-%b-%Y") AS 'Sale Activation Date',
DATE_FORMAT(A.expiry_date, "%d-%b-%Y") AS 'Sale Valid for use Date', C.first_name AS 'Account Manager Name',
A.id AS 'Transaction ID', REPLACE(A.sales_order_no, " ","") AS 'Sales Order No.', A.company_account_id AS 'Customer ID',
REPLACE(D.company_name, ',', ';') AS 'Customer Name', E.city_desc AS 'Location', ROUND(A.final_sale_price) AS 'Value of Product',
A.credit_period AS 'CreditPeriod', Round(A.discount_rate) AS '% Discount', tax AS 'ServiceTax', A.enabled AS 'ActivationStatus',
F.text AS 'SaleStatus',
CASE 
	WHEN D.enabled = 1 Then 'Activated'
	WHEN D.enabled = 2 THEN 'Suspended'
	WHEN D.enabled = 3 THEN 'Deactivated'
	WHEN D.enabled = '' THEN 'N/A'
	END AS Company_Status,
CASE
	WHEN D.verified = 1 THEN 'Backend Verified'
	WHEN D.verified = 2 THEN 'Service Verified'
	WHEN D.verified = 3 THEN 'Submitted'
	WHEN D.verified = 4 THEN 'On hold'
	WHEN D.verified = 5 THEN 'Rejected'
	WHEN D.verified = '' THEN 'N/A'
	END AS Verification_Type 

FROM backoffice_accountsales A
LEFT JOIN backoffice_package B
ON A.package_id = B.id
LEFT JOIN auth_user C
ON A.sales_manager_id = C.id
LEFT JOIN backoffice_companyaccount D
ON A.company_account_id = D.id
LEFT JOIN lookup_city_distinct E
ON D.location = E.city_id
LEFT JOIN SumoPlus_Complement.lookup_enabled F
ON A.enabled = F.id;
-- Where A.id IN (129, 5706, 5707, 57925, 57926, 57933, 57973)
-- WHERE DATE(A.created_date) = DATE_ADD(CURDATE(), INTERVAL -1 DAY)
-- WHERE A.created_date BETWEEN '2011-06-24' AND '2011-07-17'
;
