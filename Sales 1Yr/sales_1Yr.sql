SELECT sales_order_no,REPLACE(company_name,',',';') as company_name,A.enabled,C.text as Status,A.deactivation_date,A.suspension_date,A.created_date
from backoffice_accountsales A
left join backoffice_companyaccount B
on A.company_account_id = B.id
left join SumoPlus_Complement.lookup_enabled C
on A.enabled = C.id
where A.created_date >= DATE_SUB(CURDATE(),INTERVAL 365 day)
