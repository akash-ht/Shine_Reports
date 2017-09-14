
/*************************        
Author: Shailendra
Description: VendorPivot
Date: Sat Jun  2 11:37:04 2012
***************************/

libname ConsLib "/data/Analytics/Utils/consolidatedDB/Model/SASTemp";
libname sasTemp "/data/Shine/MarketingReports/Model/SASTemp";
libname CQSLib "/data/Shine/CQS/Model/SASTemp/";

/*
DATA VendorDetails_Large;
	SET ConsLib.consolidatedDB(KEEP = sa_UserId sa_Email sa_Status StartVendorId EndVendorId sa_Industry da_RegistrationStartDate 
	da_RegistrationEndDate sa_City TotalExperience sa_StartSource sa_Source sa_Function sa_experience);
	IF ( DATEPART(da_RegistrationStartDate) >= INTNX('month', today(), -1) OR DATEPART(da_RegistrationEndDate) >= INTNX('month', today(), -1) )
	AND ( DATEPART(da_RegistrationStartDate) < INTNX('day', today(), -1) OR DATEPART(da_RegistrationEndDate) < INTNX('day', today(), -1) );
RUN;

DATA Input1(KEEP = sa_UserId);
        SET VendorDetails_Large;
        IF sa_Status = 'Activated' AND sa_experience = 0;
RUN;

PROC SORT DATA = Input1; BY sa_UserId; RUN;
PROC SORT DATA = consLib.CandPrefFA; BY UserId; RUN;
DATA Output1(SORTEDBY=UserId);
        MERGE Input1(IN=A RENAME=(sa_UserId=UserId)) consLib.CandPrefFA(DROP = DesiredSubFunction);
        BY UserId;
        IF A;
RUN;

PROC SORT DATA = Output1 OUT = Output2 NODUPKEY; BY UserId DesiredFunction; RUN;
DATA Output3;
        SET Output2;
        BY UserId;
        RETAIN n;
        IF First.UserId THEN n = 1;
        ELSE n = n + 1;
RUN;

DATA Output4;
        SET Output3;
        IF n <= 10;
RUN;

PROC TRANSPOSE DATA = Output4 OUT = Output5 (DROP = _NAME_);
        VAR DesiredFunction;
        BY UserId;
RUN;

DATA Output6(DROP = Col1 Col2 Col3 Col4 Col5 Col6 Col7 Col8 Col9 Col10);
        FORMAT Desired_Functions $1010.;
        SET Output5;
        Desired_Functions = COMPBL(CATX('|', Col1, Col2, Col3, Col4, Col5, Col6, Col7, Col8, Col9, Col10));
RUN;

PROC SORT DATA = Output6; BY UserId; RUN;
PROC SORT DATA = VendorDetails_Large; BY sa_UserId; RUN;
DATA sasTemp.VendorDetails_Large(DROP = sa_experience);
	MERGE VendorDetails_Large(IN=A RENAME = (sa_UserId = UserId)) Output6;
	BY UserId;
	IF A;
RUN;

*/

%let dayBefore = -1;
DATA VendorDetails_LastDay(DROP = da_LastModified);
	SET ConsLib.consolidatedDBinc(KEEP = sa_UserId sa_Email sa_Status StartVendorId EndVendorId sa_Industry da_RegistrationStartDate 
	da_RegistrationEndDate sa_City TotalExperience sa_StartSource sa_Source sa_Function da_LastModified sa_experience);
	IF ( DATEPART(da_RegistrationStartDate) = INTNX('day', today(), &dayBefore) OR DATEPART(da_RegistrationEndDate) = INTNX('day', today(), &dayBefore) ) OR 
	( ( DATEPART(da_RegistrationStartDate) >= INTNX('month', today(), 0) OR DATEPART(da_RegistrationEndDate) >= INTNX('month', today(), 0) )
	AND DATEPART(da_LastModified) = INTNX('day', today(), &dayBefore) );
RUN;

/*	Prefered FA Start	*/
DATA Input1(KEEP = sa_UserId);
        *SET consLib.consolidateddbinc;
        SET VendorDetails_LastDay;
        IF sa_Status = 'Activated' AND sa_experience = 0;
        IF DatePart(da_RegistrationEndDate) = INTNX('day', TODAY(), &dayBefore);
RUN;

PROC SORT DATA = Input1; BY sa_UserId; RUN;
PROC SORT DATA = consLib.CandPrefFAInc; BY UserId; RUN;
DATA Output1(SORTEDBY=UserId);
        MERGE Input1(IN=A RENAME=(sa_UserId=UserId)) consLib.CandPrefFAInc(DROP = DesiredSubFunction);
        BY UserId;
        IF A;
RUN;

PROC SORT DATA = Output1 OUT = Output2 NODUPKEY; BY UserId DesiredFunction; RUN;
DATA Output3;
        SET Output2;
        BY UserId;
        RETAIN n;
        IF First.UserId THEN n = 1;
        ELSE n = n + 1;
RUN;

DATA Output4;
        SET Output3;
        IF n <= 10;
RUN;

PROC TRANSPOSE DATA = Output4 OUT = Output5 (DROP = _NAME_);
        VAR DesiredFunction;
        BY UserId;
RUN;

DATA Output6(DROP = Col1 Col2 Col3 Col4 Col5 Col6 Col7 Col8 Col9 Col10);
        FORMAT Desired_Functions $1010.;
        SET Output5;
        Desired_Functions = COMPBL(CATX('|', Col1, Col2, Col3, Col4, Col5, Col6, Col7, Col8, Col9, Col10));
RUN;

PROC SORT DATA = Output6; BY UserId; RUN;
PROC SORT DATA = VendorDetails_LastDay; BY sa_UserId; RUN;
DATA sasTemp.VendorDetails_LastDay;
	MERGE VendorDetails_LastDay(IN=A RENAME = (sa_UserId = UserId)) Output6;
	BY UserId;
	IF A;
RUN;

/*	Prefered FA End		*/

DATA _NULL_;
	SET sasTemp.VendorDetails_LastDay(OBS=1);
	IF DAY(today()) > 1 AND DAY(today()) < 8 THEN DO;
		CALL SYMPUT('skip_LastWeek', '') ;
		CALL SYMPUT('skip_MTD', 'CANCEL') ;
	END;
	ELSE DO;
		CALL SYMPUT('skip_LastWeek', 'CANCEL') ;
		CALL SYMPUT('skip_MTD', '') ;
	END;
RUN;

PROC SORT DATA = sasTemp.VendorDetails_LastDay; BY UserId; RUn;
PROC SORT DATA = sasTemp.VendorDetails_Large; BY UserId; RUn;
DATA sasTemp.VendorDetails_Large(SORTEDBY=UserId);
	MERGE sasTemp.VendorDetails_Large sasTemp.VendorDetails_LastDay;
	BY UserId;
	IF DATEPART(da_RegistrationStartDate) >= INTNX('month', today(), -1) OR DATEPART(da_RegistrationEndDate) >= INTNX('month', today(), -1);
RUN;

DATA sasTemp.VendorDetails_MTD;
	SET sasTemp.VendorDetails_Large;
	IF DATEPART(da_RegistrationStartDate) >= INTNX('month', INTNX('day', today(), -&dayBefore), 0) OR DATEPART(da_RegistrationEndDate) >= INTNX('month', INTNX('day', today(), -&dayBefore), 0);
RUN;

DATA sasTemp.VendorDetails_LastWeek;
	SET sasTemp.VendorDetails_Large;
	IF DATEPART(da_RegistrationStartDate) >= INTNX('day', today(), -7) OR DATEPART(da_RegistrationEndDate) >= INTNX('day', today(), -7);
RUN;

DATA VendorDetails1;
	SET sasTemp.VendorDetails_LastWeek;
RUN &skip_LastWeek;

DATA VendorDetails1;
	SET sasTemp.VendorDetails_MTD;
RUN &skip_MTD;


PROC SORT DATA = VendorDetails1; BY UserId; RUN;
PROC SORT DATA = CQSLib.CandidateDB; BY UserId; RUN;
DATA VendorDetails2;
        MERGE VendorDetails1(IN=A) CQSLib.CandidateDB(KEEP = UserId S RENAME = (S = CandidateQualityScore));
        BY UserId;
        IF A;
RUN;

DATA VendorDetails3(RENAME = (sa_Email = Email sa_Status = Status sa_Industry = Industry1 sa_Function = Function sa_City = City 
sa_StartSource = StartVendor sa_Source = EndVendor));
	RETAIN UserId sa_Email sa_Status StartVendorId EndVendorId sa_Industry sa_Function Desired_Functions 
	RegistrationStartDate RegistrationEndDate sa_City TotalExperience sa_StartSource sa_Source CandidateQualityScore;
	SET VendorDetails2;
	RegistrationStartDate = da_RegistrationStartDate / 86400 ;
	RegistrationEndDate = da_RegistrationEndDate / 86400 ;
	CandidateQualityScore = ROUND(CandidateQualityScore,.1);
	FORMAT RegistrationStartDate Date9.;
	FORMAT RegistrationEndDate Date9.;
	DROP da_RegistrationStartDate da_RegistrationEndDate;
RUN;

%let previousDate = %SYSFUNC(INTNX(day, %SYSFUNC(today()), -1), YYMMDD10.);
PROC EXPORT DATA = VendorDetails3 OUTFILE = "/data/Shine/MarketingReports/Output/&previousDate._vendor_pivot.csv" DBMS = CSV REPLACE; RUN;
