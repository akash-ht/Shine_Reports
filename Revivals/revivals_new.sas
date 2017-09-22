LIBNAME consLIB "/data/Analytics/Utils/consolidatedDB/Model/SASTemp";
LIBNAME mylib "/data/Shine/CareerProgressionTool/Model/SASTemp";


DATA mylib.temp(DROP = sa_Status da_LastLogin);
	SET conslib.consolidateddb(KEEP = sa_UserId sa_Status da_LastLogin);
	IF sa_Status='Activated' AND DATEPART(da_LastLogin)<INTNX('day',Today(),-183);
	FORMAT LastLogin DATE9.;
	LastLogin = DATEPART(da_LastLogin);
RUN;

PROC SORT DATA = mylib.temp;BY sa_UserId;RUN;
