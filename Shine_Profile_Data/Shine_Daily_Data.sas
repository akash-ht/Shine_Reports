OPTIONS OBS = MAX COMPRESS = YES;
LIBNAME consDB '/data/Analytics/Utils/consolidatedDB/Model/SASTemp';

%let curdate = today();
%let file_date = intnx('day',&curdate.,-1);

DATA Boxx_Shine_Data;
	set consDB.consolidatedDB(KEEP = sa_UserId sa_FirstName sa_LastName sa_CellPhone sa_Status da_LastLogin 
									da_LastModified da_LastAppliedDate da_RegistrationStartDate 
									da_RegistrationEndDate StartVendorId EndVendorId sa_Source 
									sa_SalaryMax fa_SpamStatus fa_BounceStatus sa_JobTitle sa_SubFunction sa_Industry 
									sa_Function fa_EmailAlertStatus fa_SMSAlertFlag sa_City sa_State sa_Gender 
									);
	if  da_LastLogin <=  INTNX('DAY', TODAY(), -1);
RUN;

PROC EXPORT DATA = Boxx_Shine_Data OUTFILE = '/backup/shine_data.csv' DBMS = CSV REPLACE ; RUN ;
