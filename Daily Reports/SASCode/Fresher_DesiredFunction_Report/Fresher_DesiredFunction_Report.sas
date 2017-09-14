
/*************************        
Author: Shailendra
Description: 3 Months Freshers' Desired Function Wise Numbers
Date: Fri May  4 16:46:02 2012
***************************/

libname consLib '/data/Analytics/Utils/consolidatedDB/Model/SASTemp';
libname LuLib '/data/Analytics/Utils/consolidatedDB/Model/Lookups';

%let dayBefore = -1;

DATA Input1(KEEP = sa_UserId sa_Source);
        SET consLib.consolidateddbinc;
        IF sa_Status = 'Activated' AND sa_experience = 0;
        IF DatePart(da_RegistrationEndDate) = INTNX('day', TODAY(), &dayBefore);
RUN;

PROC SORT DATA = Input1; BY sa_UserId; RUN;
/*PROC SORT DATA = consLib.CandPrefFA; BY UserId; RUN;*/
PROC SORT DATA = consLib.CandPrefFAInc; BY UserId; RUN;
DATA Output1(SORTEDBY=UserId);
        MERGE Input1(IN=A RENAME = (sa_UserId=UserId)) consLib.CandPrefFAInc(DROP = DesiredSubFunction);
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

PROC SORT DATA = Output5; BY UserId; RUN;
PROC SORT DATA = Input1; BY sa_UserId; RUN;
DATA Output6(RENAME = (sa_Source = EndSource));
	MERGE Output5(IN=A) Input1(RENAME = (sa_UserId = UserId));
	BY UserId;
	IF A;
RUN;

DATA Output7(DROP = Col1 Col2 Col3 Col4 Col5 Col6 Col7 Col8 Col9 Col10);
	FORMAT Desired_Functions $1010.;
	SET Output6;
	Desired_Functions = COMPBL(CATX('|', Col1, Col2, Col3, Col4, Col5, Col6, Col7, Col8, Col9, Col10));
RUN;

DATA Output7;
	RETAIN UserId EndSource Desired_Functions;
	SET Output7;
RUN;

/*
%let reportDate = INTNX('day', TODAY(), &dayBefore);
%let DD = TRANWRD(PUT(DAY(&reportDate),2.),' ','0');
%let MM = TRANWRD(PUT(MONTH(&reportDate),2.),' ','0');
%let YYYY = PUT(YEAR(&reportDate),4.);
%let date = CATX('-',&YYYY,&MM,&DD);
%let exportFile = CATS('"/data/Shine/MarketingReports/Output/Fresher_DesiredFunction_',&date,'.csv"');
*/

%let previousDate = %SYSFUNC(INTNX(day, %SYSFUNC(today()), -1), YYMMDD10.);
%let exportFile = "/data/Shine/MarketingReports/Output/Fresher_DesiredFunction_&previousDate..csv";
PROC EXPORT DATA = Output7 OUTFILE = &exportFile DBMS = CSV REPLACE; RUN;


/*
proc sql; 
	SELECT COUNT(distinct userid) from consLib.CandPrefFA;
	CREATE TABLE TEMP AS SELECT UserId , COUNT(*) AS FAs FROM consLib.CandPrefFA GROUP BY 1;
	CREATE TABLE Morethan10FA AS SELECT * FROM TEMP WHERE FAs > 10;
QUIT;
PROC EXPORT DATA = Morethan10FA OUTFILE = '/data/Shine/MarketingReports/Output/Morethan10FA.csv' DBMS = CSV REPLACE; RUN;
proc sql; 
	SELECT COUNT(distinct userid) from consLib.CandPrefInd;
	CREATE TABLE TEMP AS SELECT UserId , COUNT(*) AS Inds FROM consLib.CandPrefInd GROUP BY 1;
	CREATE TABLE Morethan10Ind AS SELECT * FROM TEMP WHERE Inds > 10;
QUIT;
PROC EXPORT DATA = Morethan10Ind OUTFILE = '/data/Shine/MarketingReports/Output/Morethan10Ind.csv' DBMS = CSV REPLACE; RUN;
*/
