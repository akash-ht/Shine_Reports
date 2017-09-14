SELECT A.company_account_id AS "Account ID", A.sales_order_no AS "Sales Order No", A.id AS "Transaction ID", A.package_id AS "Package ID",
B.price AS "Basic Price", A.discount_rate AS "Discount %", B.price * A.discount_rate / 100 AS "Discount Value",
B.price * (1-A.discount_rate/100) AS "Net Price", A.tax AS "Service Tax", A.final_sale_price AS "Actual Sales Price",
A.credit_period AS "Credit Period", A.expiry_date AS "Valid Until", A.bank_reference_no AS "Bank Ref No",
A.sales_order_no as "Sales Order No", A.created_date AS "Sales Date"
FROM backoffice_accountsales A
LEFT JOIN backoffice_package B
ON A.package_id = B.id
WHERE DATE(A.created_date) > '2011-04-01'
;

/*
SELECT A.company_account_id AS "Account ID", A.id AS "Sales ID", A.package_id AS "Package ID",
B.price AS "Basic Price", A.discount_rate AS "Discount %", B.price * A.discount_rate / 100 AS "Discount Value",
B.price * (1-A.discount_rate/100) AS "Net Price", A.tax AS "Service Tax", A.final_sale_price AS "Actual Sales Price",
A.credit_period AS "Credit Period", A.expiry_date AS "Valid Until", A.bank_reference_no AS "Bank Ref No",
A.sales_order_no as "Sales Order No", A.created_date AS "Sales Date"
FROM backoffice_accountsales A
LEFT JOIN backoffice_package B
ON A.package_id = B.id 
WHERE DATE(A.created_date) > '2011-04-01'
;
*/
