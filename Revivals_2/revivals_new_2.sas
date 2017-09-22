LIBNAME consLIB "/data/Analytics/Utils/consolidatedDB/Model/SASTemp";
LIBNAME mylib "/data/Shine/CareerProgressionTool/Model/SASTemp";

/*
DATA mylib.temp(DROP = sa_Status da_LastLogin);
	SET conslib.consolidateddb(KEEP = sa_UserId sa_Status da_LastLogin);
	IF sa_Status='Activated' AND DATEPART(da_LastLogin)<INTNX('day',Today(),-183);
	FORMAT LastLogin DATE9.;
	LastLogin = DATEPART(da_LastLogin);
RUN;

PROC SORT DATA = mylib.temp;BY sa_UserId;RUN;
*/

DATA temp(DROP = da_RegistrationEndDate da_LastModified);
	SET conslib.consolidateddbinc(KEEP = sa_UserId da_RegistrationEndDate da_LastModified);
	IF DATEPART(da_RegistrationEndDate)<INTNX('day',Today(),-183) AND DATEPART(da_LastModified)<INTNX('day',Today(),-183);
RUN;

PROC SORT DATA = temp;BY sa_UserId;RUN;

DATA mylib.mergetemp;
	MERGE temp(IN = A) mylib.temp(IN = B);
	BY sa_UserId;
	IF A AND B;
RUN;

%let previousDate = %SYSFUNC(INTNX(day, %SYSFUNC(today()), -1), YYMMDD10.);
PROC EXPORT DATA = mylib.mergetemp OUTFILE = "/data/Shine/CareerProgressionTool/Output/revivals_&previousDate..csv" DBMS = CSV REPLACE; RUN;
/*
PROC EXPORT DATA = mylib.mergetemp OUTFILE = '/data/Shine/CareerProgressionTool/Output/revivals_chk_29sep.csv' DBMS=csv REPLACE; RUN;
