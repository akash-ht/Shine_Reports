SELECT A.id,REPLACE(company_name,',',';')AS company_name,
case account_type
when 0 THEN "Company"
when 1 THEN "Consultant"
when 2 THEN "Others"
when 3 THEN "Enterprise"
ELSE "Not Specified"
END AS account_type,
sales_order_no,REPLACE(E.username,',',';') AS sales_manager, C.final_sale_price as finalSalePrice,C.package_id as package_id,D.name AS Package_name, 
C.expiry_date,C.created_date
from SumoPlus.backoffice_companyaccount A
left join SumoPlus_Complement.ClientType B
on A.id=B.company_account_id
left join SumoPlus.backoffice_accountsales C
on A.id = C.company_account_id
left join SumoPlus.backoffice_package D
on C.package_id = D.id
left join auth_user E
on C.sales_manager_id = E.id
where C.enabled=1 and A.enabled=1 AND company_name NOT LIKE 'Shine Demo%'
group by  A.id,company_name,account_type,sales_order_no,sales_manager,finalSalePrice,package_id,C.expiry_date,C.created_date
