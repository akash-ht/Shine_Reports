select id, rec_name, email, mobile_no, report_type, date_entry, IFNULL(city,'') AS City, IFNULL(source,'') AS Source, REPLACE(REPLACE(organization,'\n',''),'\t','') AS Organization
From recruiter_shinemyparichay
where date_entry >= DATE_SUB(CURDATE(), INTERVAL 4 DAY) AND date_entry < DATE_SUB(CURDATE(), INTERVAL 0 DAY)
-- where date_entry >= DATE_SUB(CURDATE(), INTERVAL 5 DAY) AND date_entry < DATE_SUB(CURDATE(), INTERVAL 2 DAY)
