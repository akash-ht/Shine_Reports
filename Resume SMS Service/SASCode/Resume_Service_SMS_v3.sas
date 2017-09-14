libname FlatLib '/data/Analytics/Utils/dataTransfer/Model/SASTemp/CandidateDataSets';
libname sasTemp '/data/Analytics/Utils/MarketingReports/Model/SASTemp';
libname CIncLib  "/data/Analytics/Utils/dataTransfer/Model/SASTemp/CandidateDataSets/Incrementals";
libname consLib '/data/Analytics/Utils/consolidatedDB/Model/SASTemp';
	
/*
Data sastemp.cold_calling (keep=FirstName Email CellPhone TotalExperience Salary City LastLogin);
	set CIncLib.flatfile1;
*/

DATA sasTemp.ResumeServiceExcludeList;
	INFILE '/data/Analytics/Utils/MarketingReports/Input/ResumeServiceExcludeList.csv'
        DLM=',' MISSOVER DSD LRECL=3326 FIRSTOBS=1 DSD termstr=lf;
	INFORMAT Email $100.;
	FORMAT Email $100.;
	INPUT Email $;
RUN;

/*Industry, Exp and Sal rules*/
DATA sasTemp.YesterdayData(RENAME = (sa_CellPhone = CellPhone sa_Email = Email));
	SET consLib.consolidatedDbinc;
	IF DATEPART(da_LastLogin) >= INTNX('day', TODAY(), -1) AND DATEPART(da_LastLogin) < INTNX('day', TODAY(), 0);
	/*IF sa_Experience ~= . AND sa_Experience >= 3;*/
	IF sa_Experience ~= . AND sa_Experience >= 5;
	IF sa_SalaryMin ~= . AND sa_SalaryMin >= 5;
	IF sa_Industry NOT IN ('Education/ Training','Medical/ Healthcare');
	IF fa_SMSAlertFlag = 'Y';
	KEEP sa_CellPhone sa_Email;
RUN;

/*
DATA sasTemp.YesterdayData_v1(RENAME = (sa_CellPhone = CellPhone sa_Email = Email));
        SET consLib.consolidatedDbinc;
        IF DATEPART(da_LastLogin) >= INTNX('day', TODAY(), -1) AND DATEPART(da_LastLogin) < INTNX('day', TODAY(), 0);
        /*IF sa_Experience ~= . AND sa_Experience >= 3;*/
  /*      IF sa_Experience ~= . AND sa_Experience >= 1 AND sa_Experience < 4;
	IF sa_SalaryMin ~= . AND sa_SalaryMin >= 2;
	IF sa_Industry NOT IN ('Education/ Training','Medical/ Healthcare');
        IF fa_SMSAlertFlag = 'Y';
        KEEP sa_CellPhone sa_Email;
RUN;

/*
DATA sasTemp.YesterdayData;
	*SET FlatLib.CandidateStatic;
	SET CIncLib.CandidateStatic;
	IF DATEPART(LastLogin) >= INTNX('day', TODAY(), -1) AND DATEPART(LastLogin) < INTNX('day', TODAY(), 0);
	IF Experience ~= . AND Experience >= 6;		*6 is lookup of 3 Yrs;
	IF Salary ~= . AND Salary >= 7;			*9 is lookup of Rs 4.0 - 4.5 Lakh / Yr ;
	IF SMSAlertFlag = 1;
	KEEP CellPhone Email;
RUN;
*/
/* Mobile rules*/
DATA sasTemp.YesterdayData2;
	SET sasTemp.YesterdayData;
	CellPhone2 = Compress(CellPhone, '()-+ ');	*remove characters from mobile;
	L = LENGTH(CellPhone2);
	IF L >= 10;
	CellPhone3 = SUBSTR(CellPhone2, L - 9, 10);	*last 10 characters of mobile;
	FirstDigit = SUBSTR(CellPhone3, 1, 1);
	IF FirstDigit >= 4;
	SentDate = today();
	FORMAT SentDate Date9.;
	KEEP CellPhone3 Email SentDate;
	RENAME CellPhone3 = CellPhone;
RUN;
/*
DATA sasTemp.YesterdayData2_v1;
        SET sasTemp.YesterdayData_v1;
        CellPhone2 = Compress(CellPhone, '()-+ ');      *remove characters from mobile;
        L = LENGTH(CellPhone2);
        IF L >= 10;
        CellPhone3 = SUBSTR(CellPhone2, L - 9, 10);     *last 10 characters of mobile;
        FirstDigit = SUBSTR(CellPhone3, 1, 1);
        IF FirstDigit >= 4;
        SentDate = today();
        FORMAT SentDate Date9.;
        KEEP CellPhone3 Email SentDate;
        RENAME CellPhone3 = CellPhone;
RUN;

/*
DATA sasTemp.ResumeServiceSMSSent;
        INFILE '/data/Analytics/Utils/MarketingReports/Input/RService_SMS_Sent.csv'
        DLM=',' MISSOVER DSD LRECL=3326 FIRSTOBS=2 DSD termstr=crlf;
        INFORMAT Email $100.;
        INFORMAT CellPhone $50.;
        INFORMAT SentDate ddmmyy10.;
        FORMAT Email $100.;
        FORMAT CellPhone $50.;
        FORMAT SentDate Date9.;
        INPUT Email CellPhone SentDate;
RUN;
DATA sasTemp.ResumeServiceSMSSent;
	SET sasTemp.ResumeServiceSMSSent;
	CellPhone2 = Compress(CellPhone, '()-+ ');      *remove characters from mobile;
        L = LENGTH(CellPhone2);
        IF L >= 10;
        CellPhone3 = SUBSTR(CellPhone2, L - 9, 10);     *last 10 characters of mobile;
        FirstDigit = SUBSTR(CellPhone3, 1, 1);
        IF FirstDigit >= 4;
	KEEP CellPhone3 Email SentDate;
	RENAME CellPhone3 = CellPhone;
RUN;
*/

DATA sasTemp.ResumeServiceSMSSent;
	SET sasTemp.ResumeServiceSMSSent;
	IF SentDate >= INTNX('day', today(), -15) AND SentDate < INTNX('day', today(), 0);
RUN;
/*
DATA sasTemp.ResumeServiceSMSSent_v1;
        SET sasTemp.ResumeServiceSMSSent_v1;
	IF SentDate >= INTNX('day', today(), -15) AND SentDate < INTNX('day', today(), 0);
RUN;
*/
PROC SORT DATA = sasTemp.ResumeServiceSMSSent; BY Email; RUN;
PROC SORT DATA = sasTemp.YesterdayData2; BY Email; RUN;
DATA sasTemp.YesterdayData3;
	MERGE sasTemp.ResumeServiceSMSSent(IN=A) sasTemp.YesterdayData2(IN=B);
	BY Email;
	IF B AND NOT A;
RUN;

/*
PROC SORT DATA = sasTemp.ResumeServiceSMSSent_v1; BY Email; RUN;
PROC SORT DATA = sasTemp.YesterdayData2_v1; BY Email; RUN;
DATA sasTemp.YesterdayData3_v1;
	MERGE sasTemp.ResumeServiceSMSSent_v1(IN=A) sasTemp.YesterdayData2_v1(IN=B);
        BY Email;
        IF B AND NOT A;
RUN;

/*
DATA sasTemp.YesterdayData3_v1;
	SET sasTemp.YesterdayData2_v1;
RUN;
*/

PROC SORT DATA = sasTemp.YesterdayData3; BY Email; RUN;
PROC SORT DATA = sasTemp.ResumeServiceExcludeList; BY Email; RUN;

DATA YesterdayData3;
        MERGE sasTemp.YesterdayData3(IN = A)sasTemp.ResumeServiceExcludeList(IN = B);
	BY Email;
	IF A AND NOT B;
RUN;
/*
PROC SORT DATA = sasTemp.YesterdayData3_v1; BY Email; RUN;
DATA YesterdayData3_v1;
        MERGE sasTemp.YesterdayData3_v1(IN = A)sasTemp.ResumeServiceExcludeList(IN = B);
        BY Email;
        IF A AND NOT B;
RUN;
*/
DATA OUTPUT(KEEP = CellPhone Email);
	RETAIN CellPhone Email;
	/*SET sasTemp.YesterdayData3;*/
	SET YesterdayData3;
RUN;

PROC EXPORT DATA = OUTPUT OUTFILE = '/data/Analytics/Utils/MarketingReports/Output/ResumeService/rServiceReport.csv' DBMS = CSV REPLACE; PUTNAMES = NO; RUN;

DATA sasTemp.ResumeServiceSMSSent;
	SET sasTemp.ResumeServiceSMSSent sasTemp.YesterdayData3;
RUN;
/*PROC SORT DATA = sasTemp.ResumeServiceSMSSent NODUPKEY;  BY Email; RUN;*/
/*
DATA OUTPUT(KEEP = CellPhone Email);
        RETAIN CellPhone Email;
        SET YesterdayData3_v1;
RUN;

PROC EXPORT DATA = OUTPUT OUTFILE = '/data/Analytics/Utils/MarketingReports/Output/ResumeService/rServiceReport_1_4yrs.csv' DBMS = CSV REPLACE; PUTNAMES = NO; RUN;

DATA sasTemp.ResumeServiceSMSSent_v1;
        SET sasTemp.ResumeServiceSMSSent_v1 sasTemp.YesterdayData3_v1;
RUN;
/*
DATA sasTemp.ResumeServiceSMSSent_v1;
        SET sasTemp.YesterdayData3_v1;
RUN;
*/
