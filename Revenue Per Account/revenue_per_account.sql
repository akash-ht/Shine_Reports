Select A.company_account_id, A.TotalSalePriceAllSales, REPLACE(B.company_name,',',';') AS company_name, B.sales_manager_id, B.account_type, REPLACE(B.city,',',';') AS city, B.contact_email from ShineReport.AccountType A
lefT JOIN SumoPlus.backoffice_companyaccount B
ON A.company_account_id = B.id
Where A.TotalSalePriceAllSales != 0;
