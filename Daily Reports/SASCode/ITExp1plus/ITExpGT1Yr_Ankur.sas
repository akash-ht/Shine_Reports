LIBNAME Mylib '/data/Shine/Shine_AdHoc/Model/SASTemp';
LIBNAME consDB  '/data/Analytics/Utils/consolidatedDB/Model/SASTemp';


DATA temp;
	SET consDB.consolidateddb(KEEP =  sa_Status sa_Function TotalExperience da_LastLogin sa_UserId da_LastLogin da_RegistrationEndDate);
	IF sa_Status = 'Activated' AND DATEPART(da_LastLogin) >= INTNX('day', today(), -183) AND (sa_Function = 'IT - Hardware / Networking / Telecom Engineering' OR sa_Function = 'IT - Software') AND  TotalExperience ~= '' AND TotalExperience ~= '0 Yr';
RUN;

/*
PROC SORT DATA = temp; BY TotalExperience;
PROC MEANS DATA = temp noprint;
	BY TotalExperience;
	VAR sa_SalaryMin;
	OUTPUT OUT =temp1
	MEAN(sa_SalaryMin) =meansal;
RUN;

PROC EXPORT DATA = temp1 OUTFILE = '/data/Shine/Shine_AdHoc/Output/ITactivedbtemp.csv' DBMS = CSV REPLACE;RUN;
*/

PROC SQL;
CREATE TABLE temp2 As
SELECT 'ProfileCompletions',Count(sa_UserId) from temp
WHERE DATEPART(da_RegistrationEndDate) = INTNX('day', today(), -1)
UNION ALL
/* Exiting Today */
SELECT 'ExitingToday' ,Count(sa_UserId) from temp
where DATEPART(da_LastLogin) = INTNX('day', today(), -183) 
UNION ALL
/* ActiveDB */
SELECT 'ActiveDB',Count(sa_UserId) from temp;
QUIT;

/*PROC EXPORT DATA = temp2 OUTFILE = '/data/Shine/Shine_AdHoc/Output/ITExpGT1Yr_Ankur.csv' DBMS = CSV REPLACE;RUN;	*/
%let previousDate = %SYSFUNC(INTNX(day, %SYSFUNC(today()), -1), YYMMDD10.);
PROC EXPORT DATA = temp2 OUTFILE = "/data/Shine/Shine_AdHoc/Output/&previousDate._ITExp1plus.csv" DBMS = CSV REPLACE; RUN;
