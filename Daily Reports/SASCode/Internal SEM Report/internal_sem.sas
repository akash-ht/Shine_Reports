
/*************************        
Author: Shailendra
Description: InternalSEM
Date: Sat Jun  2 11:37:04 2012
***************************/

libname ConsLib "/data/Analytics/Utils/consolidatedDB/Model/SASTemp";
libname sasTemp "/data/Shine/MarketingReports/Model/SASTemp";
*libname CQSLib "/data/Shine/CQS/Model/SASTemp/";

/*
DATA InternalSEM_MTD;
	SET ConsLib.consolidatedDB(KEEP = sa_UserId sa_Email sa_FirstName sa_CellPhone sa_City sa_Status EndVendorId sa_Source da_RegistrationEndDate
	sa_Industry TotalExperience sa_SubFunction sa_Function sa_experience);
	IF DATEPART(da_RegistrationEndDate) >= INTNX('month', today(), 0) AND DATEPART(da_RegistrationEndDate) < INTNX('day', today(), -1);
	IF sa_Source = 'Internal SEM';
RUN;

DATA Input1(KEEP = sa_UserId);
        SET InternalSEM_MTD;
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
PROC SORT DATA = InternalSEM_MTD; BY sa_UserId; RUN;
DATA sasTemp.InternalSEM_MTD(DROP = sa_experience);
	MERGE InternalSEM_MTD(IN=A RENAME = (sa_UserId = UserId)) Output6;
	BY UserId;
	IF A;
RUN;
*/

%let dayBefore = -1;
DATA InternalSEM_LastDay(DROP = da_LastModified);
        SET ConsLib.consolidatedDBinc(KEEP = sa_UserId sa_Email sa_FirstName sa_CellPhone sa_City sa_Status EndVendorId sa_Source da_RegistrationEndDate
        sa_Industry TotalExperience sa_SubFunction sa_Function sa_experience da_LastModified);
        IF DATEPART(da_RegistrationEndDate) = INTNX('day', today(), &dayBefore) OR ( DATEPART(da_RegistrationEndDate) >= INTNX('month', today(), 0) AND DATEPART(da_LastModified) = INTNX('day', today(), &dayBefore) );
        IF sa_Source = 'Internal SEM';
RUN;

/*	Prefered FA Start	*/
DATA Input1(KEEP = sa_UserId sa_Source);
        SET InternalSEM_LastDay;
        IF sa_Status = 'Activated' AND sa_experience = 0;
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
PROC SORT DATA = InternalSEM_LastDay; BY sa_UserId; RUN;
DATA sasTemp.InternalSEM_LastDay(DROP = sa_experience);
	MERGE InternalSEM_LastDay(IN=A RENAME = (sa_UserId = UserId)) Output6;
	BY UserId;
	IF A;
RUN;
/*	Prefered FA End		*/

DATA sasTemp.InternalSEM_MTD;
	SET sasTemp.InternalSEM_MTD sasTemp.InternalSEM_LastDay;
	IF DATEPART(da_RegistrationEndDate) >= INTNX('month', today(), 0);
RUN;

/*
PROC SORT DATA = sasTemp.InternalSEM_MTD; BY UserId; RUN;
PROC SORT DATA = CQSLib.CandidateDB; BY UserId; RUN;
DATA InternalSEM2;
        MERGE sasTemp.InternalSEM_MTD(IN=A) CQSLib.CandidateDB(KEEP = UserId S RENAME = (S = CandidateQualityScore));
        BY UserId;
        IF A;
RUN;
*/

DATA InternalSEM3(RENAME = (sa_Email = Username sa_FirstName = FirstName sa_CellPhone = CellPhone sa_City = City sa_Status = Status 
sa_Source = EndVendor sa_Industry = Industry1 sa_SubFunction = SubFunction sa_Function = Function));
	RETAIN UserId sa_Email sa_FirstName sa_CellPhone sa_City sa_Status EndVendorId sa_Source CompletionDate
	TotalExperience sa_Industry sa_SubFunction sa_Function Desired_Functions;* CandidateQualityScore;
	*SET InternalSEM2;
	SET sasTemp.InternalSEM_MTD;
	CompletionDate = da_RegistrationEndDate / 86400 ;
	*CandidateQualityScore = ROUND(CandidateQualityScore,.1);
	FORMAT CompletionDate Date9.;
	DROP da_RegistrationEndDate UserId;
RUN;

%let previousDate = %SYSFUNC(INTNX(day, %SYSFUNC(today()), -1), YYMMDD10.);
PROC EXPORT DATA = InternalSEM3 OUTFILE = "/data/Shine/MarketingReports/Output/&previousDate._internal_SEM.csv" DBMS = CSV REPLACE; RUN;
