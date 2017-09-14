SELECT A.id AS "Account Id", REPLACE(company_name,',','') AS "Account Name", B.text AS "Account Status", 
created_date AS "Account Activation Date", modified_date AS "Last Modification Date", C.city_desc AS "Account Location"
FROM SumoPlus.backoffice_companyaccount A
LEFT JOIN SumoPlus_Complement.lookup_company_status B
ON A.enabled = B.id
LEFT JOIN SumoPlus.lookup_city_distinct C
ON A.location = C.city_id;
