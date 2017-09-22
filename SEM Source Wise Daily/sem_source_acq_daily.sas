LIBNAME consDB "/data/Analytics/Utils/consolidatedDB/Model/SASTemp";

DATA temp(DROP = sa_Status da_RegistrationEndDate);
	SET consDB.consolidateddbinc(KEEP = EndVendorId da_RegistrationEndDate sa_Status sa_UserId);
	WHERE sa_Status='Activated' AND DATEPART(da_RegistrationEndDate) >= INTNX('day',Today(),-1) ;
RUN;

PROC SORT DATA = temp; BY EndVendorId;RUN;
PROC SUMMARY DATA = temp; BY EndVendorId; OUTPUT OUT = temp1 (DROP = _TYPE_ RENAME= (_FREQ_=Acquisitions));RUN;

PROC EXPORT DATA = temp1 OUTFILE = '/data/Shine/MarketingReports/Output/sem_source_acq_daily.csv' DBMS=csv REPLACE;RUN;
