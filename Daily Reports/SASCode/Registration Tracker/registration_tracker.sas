
/*************************        
Author: Shailendra
Description: UPDATE consolidated DB
Date: Wed Feb 27 13:08:57 IST 2013
***************************/

libname consLib '/data/Analytics/Utils/consolidatedDB/Model/SASTemp';
libname sasTemp '/data/Shine/MarketingReports/Model/SASTemp';

%let outdir = /data/Shine/MarketingReports/Output;

DATA sasTemp.Cand0_lastday(DROP = da_RegistrationEndDate sa_state);
	SET consLib.ConsolidatedDBInc(KEEP = sa_UserId sa_Source da_RegistrationEndDate sa_city sa_state);
	RED = DatePart(da_RegistrationEndDate);
	IF RED >= INTNX('day', TODAY(), -1) AND RED < INTNX('day', TODAY(), 0);
	FORMAT TYPE $10.;
	IF sa_Source IN ('','SEO','Direct') THEN DO;
		sa_Source = 'Direct';
		Type = 'direct';
		END;
	ELSE Type = 'Paid';
	FORMAT concernCity $30.;
	IF sa_state IN ('Delhi-NCR Region') THEN concernCity = 'Delhi-NCR';
	ELSE IF sa_city IN ('Mumbai / Navi Mumbai / Thane') THEN concernCity = 'Mumbai-Thane';
	ELSE concernCity = 'Others';
RUN;

PROC SORT DATA = sasTemp.Cand0_lastday; BY Type sa_Source RED; RUN;
PROC SUMMARY DATA = sasTemp.Cand0_lastday;
	BY Type sa_Source RED; 
	OUTPUT OUT = sasTemp.Cand1_lastday(DROP = _TYPE_ RENAME = (_FREQ_ = Candidates));
RUN;

DATA sasTemp.Cand2_lastday;
	SET sasTemp.Cand1_lastday;
	IF TYPE ~= 'direct';
	Date = RED;
	FORMAT Date DATE6.;
	FORMAT RED WEEKDATE3.;
RUN;

PROC SORT DATA = sasTemp.Cand0_lastday OUT = Cand0_lastday; BY Type concernCity RED; RUN;
PROC SUMMARY DATA = Cand0_lastday;
        BY Type concernCity RED;
        OUTPUT OUT = Cand1_lastday(DROP = _TYPE_ RENAME = (_FREQ_ = Candidates));
RUN;

DATA Cand2_lastday;
        SET Cand1_lastday;
        Date = RED; 
        FORMAT Date DATE6.;
        FORMAT RED WEEKDATE3.;
RUN;

DATA sasTemp.Cand3_lastday(DROP = sa_Source concernCity);
	SET sasTemp.Cand2_lastday Cand2_lastday;
	FORMAT Type2 $10.;
	FORMAT Type3 $50.;
	LENGTH Type3 $50.;
	IF COMPBL(TYPE) = 'Direct' THEN TYPE = 'direct';
	IF concernCity = '' THEN Type2 = 'Source';
	ELSE IF sa_Source = '' THEN Type2 = 'location';
	Type3 = TRIM(COMPBL(CATX('',sa_Source,concernCity)));
RUN;

DATA sasTemp.Cand3;
	SET sasTemp.Cand3;
	IF RED ~= INTNX('day', TODAY(), -1);
RUN;

DATA sasTemp.Cand3;
	SET sasTemp.Cand3 sasTemp.Cand3_lastday;
RUN;

%let previousDate = %SYSFUNC(INTNX(day, %SYSFUNC(today()), -1), YYMMDD10.);

%include '/data/Shine/CQS/Model/SASCode/ExcelXP_tagset/ExcelXP.sas';
ods _all_ close;
*ods tagsets.ExcelXP file='Registration_Tracker.xml' path="&outdir" style=Printer;
ods tagsets.ExcelXP file="Registration_Tracker_&previousDate..xml" path="&outdir" style=BARRETTSBLUE;
PROC TABULATE DATA = sasTemp.Cand3;
        CLASS TYPE TYPE2 TYPE3 RED Date;
        Var Candidates;
        Table TYPE2 = '' * (TYPE = '' * (TYPE3 = '' ALL = 'Total' ) ALL = 'Total'), SUM = '' * Candidates = '' * Date = '' * RED = '' / BOX = 'Registrations' MISSTEXT = '0';
RUN;
ods tagsets.ExcelXP close;
