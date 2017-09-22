LIBNAME consDB "/data/Analytics/Utils/consolidatedDB/Model/SASTemp";
LIBNAME mylib "/data/Shine/Shine_AdHoc/Model/SASTemp/";

%let previousDay = -1;
DATA temp;
	SET consDB.consolidateddb(KEEP = EndVendorId StartVendorId TotalExperience da_RegistrationEndDate da_RegistrationStartDate fa_IsCellPhoneVerified fa_IsEmailVerified sa_Function sa_Status sa_UserId da_LastAppliedDate);
	/*previousDate = INTNX('day',Today(),&previousDay);
	startDay = INTNX('month',previousDate,0);*/
	/*WHERE DATEPART(da_RegistrationEndDate) >= '01JAN2017'D OR DATEPART(da_RegistrationStartDate) >= '01JAN2017'D ;*/
        WHERE DATEPART(da_RegistrationEndDate) >= '01JUN2017'D;
        /*WHERE (DATEPART(da_RegistrationEndDate) >= '01JUN2016'D AND DATEPART(da_RegistrationEndDate) <= '30JUN2016'D) ; */
	/*WHERE DATEPART(da_RegistrationEndDate) >= startDay OR DATEPART(da_RegistrationStartDate) >= startDay;*/
RUN;

DATA temp1;
	SET temp;
	WHERE sa_Status='Activated';
	IF TotalExperience IN ('0 Yr', '<1 Yr') THEN Exp_Cat = '0_1 Yr';
		ELSE IF TotalExperience IN ('1 Yr', '2 Yrs', '3 Yrs') THEN Exp_Cat = '1_3 Yr';
			 ELSE Exp_Cat = '3 Yr+';
	IF sa_Function IN ('IT - Hardware / Networking / Telecom Engineering', 'IT - Software', 'Quality / Testing (QA-QC)') THEN Func_Cat = 'IT';
		ELSE Func_Cat = 'Non-IT';
RUN;

DATA exp_temp1 exp_temp2 exp_temp3;
	SET temp1(KEEP = EndVendorId Exp_Cat);
	IF (Exp_Cat = '0_1 Yr') THEN OUTPUT exp_temp1;
	IF (Exp_Cat = '1_3 Yr') THEN OUTPUT exp_temp2;
	IF (Exp_Cat = '3 Yr+') THEN OUTPUT exp_temp3;
RUN;
	
PROC SORT DATA = exp_temp1; BY EndVendorId;RUN;
PROC SUMMARY DATA = exp_temp1; BY EndVendorId; OUTPUT OUT = exp_temp11 (DROP = _TYPE_ RENAME= (_FREQ_=Exp1));RUN;

PROC SORT DATA = exp_temp2; BY EndVendorId;RUN;
PROC SUMMARY DATA = exp_temp2; BY EndVendorId; OUTPUT OUT = exp_temp22 (DROP = _TYPE_ RENAME= (_FREQ_=Exp2));RUN;

PROC SORT DATA = exp_temp3; BY EndVendorId;RUN;
PROC SUMMARY DATA = exp_temp3; BY EndVendorId; OUTPUT OUT = exp_temp33 (DROP = _TYPE_ RENAME= (_FREQ_=Exp3));RUN;

DATA merge_exp;
	MERGE exp_temp11(IN=A) exp_temp22(IN=B) exp_temp33(IN=C);
	BY EndVendorId;
	IF A OR B OR C;
RUN;

PROC DATASETS LIB=WORK; DELETE exp_temp1 exp_temp11 exp_temp2 exp_temp22 exp_temp3 exp_temp33; RUN;	

DATA func_temp1;
	SET temp1(KEEP = EndVendorId Func_Cat);
	WHERE Func_Cat = 'IT';
RUN;

PROC SORT DATA = func_temp1; BY EndVendorId; RUN;
PROC SUMMARY DATA = func_temp1; BY EndVendorId; OUTPUT OUT = func_temp11 (DROP = _TYPE_ RENAME =(_FREQ_=IT));RUN;

DATA merge_exp2;
	MERGE merge_exp(IN = A) func_temp11(IN = B);
	BY EndVendorId;
	IF A OR B;
RUN;

PROC DATASETS LIB=WORK; DELETE func_temp1 func_temp11 merge_exp; RUN;	

DATA cellphone_verified;
	SET temp1(KEEP = fa_IsCellPhoneVerified EndVendorId);
	WHERE fa_IsCellPhoneVerified = 'Y';
RUN;

PROC SORT DATA = cellphone_verified; BY EndVendorId; RUN;
PROC SUMMARY DATA = cellphone_verified; BY EndVendorId; OUTPUT OUT = cpv (DROP = _TYPE_ RENAME =(_FREQ_=cellphone_verified));RUN;

DATA merge_exp3;
	MERGE merge_exp2(IN = A) cpv(IN = B);
	BY EndVendorId;
	IF A OR B;
RUN;

PROC DATASETS LIB=WORK; DELETE cellphone_verified cpv merge_exp2; RUN;	

DATA email_verified;
	SET temp1(KEEP = fa_IsEmailVerified EndVendorId);
	WHERE fa_IsEmailVerified = 'Y';
RUN;

PROC SORT DATA = email_verified; BY EndVendorId; RUN;
PROC SUMMARY DATA = email_verified; BY EndVendorId; OUTPUT OUT = ev (DROP = _TYPE_ RENAME =(_FREQ_=email_verified));RUN;

DATA merge_exp4;
	MERGE merge_exp3(IN = A) ev(IN = B);
	BY EndVendorId;
	IF A OR B;
RUN;

PROC DATASETS LIB=WORK; DELETE email_verified ev merge_exp3; RUN;	

DATA applied;
	SET temp1(KEEP = da_LastAppliedDate EndVendorId);
	WHERE MISSING(da_LastAppliedDate)=0;
	/*WHERE DATEPART(da_LastAppliedDate)=INTNX('day',Today(),-1);*/
RUN;

PROC SORT DATA = applied; BY EndVendorId; RUN;
PROC SUMMARY DATA = applied; BY EndVendorId; OUTPUT OUT = applied1 (DROP = _TYPE_ RENAME =(_FREQ_=Applied));RUN;

DATA merge_exp5;
	MERGE merge_exp4(IN = A) applied1(IN = B);
	BY EndVendorId;
	IF A OR B;
RUN;

PROC DATASETS LIB=WORK; DELETE applied applied1 merge_exp4; RUN;	

PROC SORT DATA = temp1; BY EndVendorId; RUN;
PROC SUMMARY DATA = temp1; BY EndVendorId; OUTPUT OUT = acquisitions (DROP = _TYPE_ RENAME =(_FREQ_=Acquisitions));RUN;

DATA merge_exp6;
	MERGE merge_exp5(IN = A) acquisitions(IN = B);
	BY EndVendorId;
	IF A OR B;
RUN;

PROC DATASETS LIB=WORK; DELETE acquisitions merge_exp5; RUN;	

DATA temp2;
	SET temp(KEEP = da_RegistrationStartDate sa_Status StartVendorId);
	WHERE sa_Status~='Activated'; /*AND DATEPART(da_RegistrationStartDate)=INTNX('day',Today(),-1);*/
RUN;

PROC SORT DATA = temp2; BY StartVendorId; RUN;
PROC SUMMARY DATA = temp2; BY StartVendorId; OUTPUT OUT = midouts (DROP = _TYPE_ RENAME =(_FREQ_=Midouts));RUN;

DATA merge_exp7;
	MERGE merge_exp6(IN = A) midouts(IN = B RENAME=(StartVendorId=EndVendorId));
	BY EndVendorId;
	IF A OR B;
RUN;

PROC DATASETS LIB=WORK; DELETE midouts merge_exp6; RUN;	

PROC SORT DATA = merge_exp7; BY EndVendorId; RUN;
PROC EXPORT DATA = merge_exp7 OUTFILE = '/data/Shine/MarketingReports/Output/source_acq_full.csv' DBMS=csv REPLACE;RUN;
