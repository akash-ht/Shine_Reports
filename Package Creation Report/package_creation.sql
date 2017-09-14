SELECT A.id AS "Package ID", A.name AS "Package Name", A.price AS "Package Price", A.package_type AS "Package Type",
A.created_date AS "Package Creation Date", A.validity AS "Valid Upto", A.duration AS "Validity (in Days)", A.no_of_login AS "No Of Logins",A.excel_monthly AS "Excel_Monthly",A.profile_monthly AS "Profile_Monthly",A.email_overall AS "Overall_Email",A.job_overall AS "Overall_Jobs"
FROM backoffice_package A
;
