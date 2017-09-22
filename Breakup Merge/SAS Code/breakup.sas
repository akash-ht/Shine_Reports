LIBNAME consdb '/data/Analytics/Utils/consolidatedDB/Model/SASTemp';

DATA activeDB(DROP = sa_Status da_LastLogin sa_Email);
	SET consDB.consolidateddb(KEEP = sa_City sa_Status da_LastLogin sa_Function sa_Industry TotalExperience sa_Salary sa_State sa_gender sa_lastEducationalQualification sa_Email);
	IF sa_Status = 'Activated' AND DATEPART(da_LastLogin) >= INTNX('day',Today(),-183) AND SCAN(sa_Email,2,'@')~='mailinator.com';
RUN;

PROC SORT DATA = activeDB; BY sa_gender; RUN;
PROC SUMMARY DATA = activeDB; BY sa_gender;OUTPUT OUT = Gendertemp(DROP = _TYPE_ RENAME = (_FREQ_ = Total_Candidates) RENAME = (sa_gender = Gender));RUN;

DATA Gendertemp;
        SET Gendertemp;
        IF Gender='N' THEN Gender='Not Specified';

PROC EXPORT DATA = Gendertemp OUTFILE = '/data/Shine/Shine_AdHoc/Output/ActiveDBbreakup/Attributes/Gender.csv' DBMS = CSV REPLACE; RUN;

PROC SORT DATA = activeDB; BY sa_lastEducationalQualification; RUN;
PROC SUMMARY DATA = activeDB; BY sa_lastEducationalQualification;OUTPUT OUT = EduQualtemp(DROP = _TYPE_ RENAME = (_FREQ_ = Total_Candidates) RENAME = (sa_lastEducationalQualification = Educational_Qualification));RUN;

DATA EduQualtemp;
        SET EduQualtemp;
        IF Educational_Qualification='' THEN Educational_Qualification='NA';
RUN;

PROC EXPORT DATA = EduQualtemp OUTFILE = '/data/Shine/Shine_AdHoc/Output/ActiveDBbreakup/Attributes/Educational_Qualification.csv' DBMS = CSV REPLACE; RUN;

PROC SORT DATA = activeDB; BY sa_State; RUN;
PROC SUMMARY DATA = activeDB; BY sa_State;OUTPUT OUT = statetemp(DROP = _TYPE_ RENAME = (_FREQ_ = Total_Candidates) RENAME = (sa_State = State));RUN;

DATA statetemp;
        SET statetemp;
        IF State='' THEN State='NA';
RUN;

PROC EXPORT DATA = statetemp OUTFILE = '/data/Shine/Shine_AdHoc/Output/ActiveDBbreakup/Attributes/State.csv' DBMS = CSV REPLACE; RUN;

PROC SORT DATA = activeDB; BY sa_City; RUN;
PROC SUMMARY DATA = activeDB; BY sa_City;OUTPUT OUT = citytemp(DROP = _TYPE_ RENAME = (_FREQ_ = Total_Candidates) RENAME = (sa_City = City));RUN;

DATA citytemp;
	SET citytemp;
	IF City='' THEN City='NA';
RUN;

PROC EXPORT DATA = citytemp OUTFILE = '/data/Shine/Shine_AdHoc/Output/ActiveDBbreakup/Attributes/City.csv' DBMS = CSV REPLACE; RUN;

PROC SORT DATA = activeDB; BY sa_Function; RUN;
PROC SUMMARY DATA = activeDB; BY sa_Function;OUTPUT OUT = fatemp(DROP = _TYPE_ RENAME = (_FREQ_ = Total_Candidates) RENAME = (sa_Function = Function)); RUN;

DATA fatemp;
	SET fatemp;
        IF Function='' THEN Function='NA';
RUN;

PROC EXPORT DATA = fatemp OUTFILE = '/data/Shine/Shine_AdHoc/Output/ActiveDBbreakup/Attributes/Function.csv' DBMS = CSV REPLACE; RUN;

PROC SORT DATA = activeDB; BY sa_Industry; RUN;
PROC SUMMARY DATA = activeDB; BY sa_Industry;OUTPUT OUT = indtemp(DROP = _TYPE_ RENAME = (_FREQ_ = Total_Candidates) RENAME = (sa_Industry = Industry)); RUN;

DATA indtemp;
	SET indtemp;
	IF Industry='' THEN Industry='NA';
RUN;

PROC EXPORT DATA = indtemp OUTFILE = '/data/Shine/Shine_AdHoc/Output/ActiveDBbreakup/Attributes/Industry.csv' DBMS = CSV REPLACE; RUN;

PROC SORT DATA = activeDB; BY TotalExperience; RUN;
PROC SUMMARY DATA = activeDB; BY TotalExperience;OUTPUT OUT = exptemp(DROP = _TYPE_ RENAME = (_FREQ_ = Total_Candidates)); RUN;

DATA exptemp;
	SET exptemp;
	IF TotalExperience='' THEN TotalExperience='NA';
RUN;

PROC EXPORT DATA = exptemp OUTFILE = '/data/Shine/Shine_AdHoc/Output/ActiveDBbreakup/Attributes/Experience.csv' DBMS = CSV REPLACE; RUN;

PROC SORT DATA = activeDB; BY sa_Salary; RUN;
PROC SUMMARY DATA =activeDB; BY sa_Salary;OUTPUT OUT = salarytemp(DROP = _TYPE_ RENAME = (_FREQ_ = Total_Candidates) RENAME = (sa_Salary = Salary)); RUN;

DATA salarytemp;
	SET salarytemp;
	IF Salary='' THEN Salary='NA';
RUN;

PROC EXPORT DATA = salarytemp OUTFILE = '/data/Shine/Shine_AdHoc/Output/ActiveDBbreakup/Attributes/Salary.csv' DBMS = CSV REPLACE; RUN;

PROC SORT DATA = activeDB; BY sa_Industry sa_Function ; RUN;
PROC SUMMARY DATA = activeDB; BY sa_Industry sa_Function;OUTPUT OUT = indfatemp(DROP = _TYPE_ RENAME = (_FREQ_ = Total_Candidates) RENAME = (sa_Industry = Industry) RENAME = (sa_Function = Function)); RUN;

DATA indfatemp;
        SET indfatemp;
        IF Industry='' THEN Industry='NA';
        IF Function='' THEN Function='NA';
RUN;

PROC EXPORT DATA = indfatemp OUTFILE = '/data/Shine/Shine_AdHoc/Output/ActiveDBbreakup/Industry/Industry_Function.csv' DBMS = CSV REPLACE; RUN;

PROC SORT DATA = activeDB; BY sa_Industry TotalExperience;RUN;
PROC SUMMARY DATA = activeDB; BY sa_Industry TotalExperience;OUTPUT OUT = indexptemp(DROP = _TYPE_ RENAME = (_FREQ_ = Total_Candidates) RENAME = (sa_Industry = Industry));RUN;

DATA indexptemp;
        SET indexptemp;
        IF Industry='' THEN Industry='NA';
        IF TotalExperience='' THEN TotalExperience='NA';
RUN;

PROC EXPORT DATA = indexptemp OUTFILE = '/data/Shine/Shine_AdHoc/Output/ActiveDBbreakup/Industry/Industry_Experience.csv' DBMS = CSV REPLACE; RUN;

PROC SORT DATA = activeDB; BY sa_Industry sa_Salary;RUN;
PROC SUMMARY DATA = activeDB; BY sa_Industry sa_Salary;OUTPUT OUT = indsaltemp(DROP = _TYPE_ RENAME = (_FREQ_ = Total_Candidates) RENAME = (sa_Industry = Industry) RENAME = (sa_Salary = Salary));RUN;

DATA indsaltemp;
        SET indsaltemp;
        IF Industry='' THEN Industry='NA';
        IF Salary='' THEN Salary='NA';
RUN;

PROC EXPORT DATA = indsaltemp OUTFILE = '/data/Shine/Shine_AdHoc/Output/ActiveDBbreakup/Industry/Industry_Salary.csv' DBMS = CSV REPLACE; RUN;

PROC SORT DATA = activeDB; BY sa_Industry sa_City;RUN;
PROC SUMMARY DATA = activeDB; BY sa_Industry sa_City;OUTPUT OUT = indcitytemp(DROP = _TYPE_ RENAME = (_FREQ_ = Total_Candidates) RENAME = (sa_Industry = Industry) RENAME = (sa_City = City));RUN;

DATA indcitytemp;
        SET indcitytemp;
        IF Industry='' THEN Industry='NA';
        IF City='' THEN City='NA';
RUN;

PROC EXPORT DATA = indcitytemp OUTFILE = '/data/Shine/Shine_AdHoc/Output/ActiveDBbreakup/Industry/Industry_City.csv' DBMS = CSV REPLACE; RUN;

PROC SORT DATA = activeDB; BY sa_Function sa_Industry; RUN;
PROC SUMMARY DATA = activeDB; BY sa_Function sa_Industry;OUTPUT OUT = faindtemp(DROP = _TYPE_ RENAME = (_FREQ_ = Total_Candidates) RENAME = (sa_Industry = Industry) RENAME = (sa_Function = Function)); RUN;

DATA faindtemp;
        SET faindtemp;
        IF Industry='' THEN Industry='NA';
        IF Function='' THEN Function='NA';
RUN;

PROC EXPORT DATA = faindtemp OUTFILE = '/data/Shine/Shine_AdHoc/Output/ActiveDBbreakup/Function/Function_Industry.csv' DBMS = CSV REPLACE; RUN;

PROC SORT DATA = activeDB; BY sa_Function sa_City; RUN;
PROC SUMMARY DATA = activeDB; BY sa_Function sa_City;OUTPUT OUT = facitytemp(DROP = _TYPE_ RENAME = (_FREQ_ = Total_Candidates) RENAME = (sa_Function = Function) RENAME = (sa_City = City));RUN;

DATA facitytemp;
        SET facitytemp;
        IF Function='' THEN Function='NA';
        IF City='' THEN City='NA';
RUN;

PROC EXPORT DATA = facitytemp OUTFILE = '/data/Shine/Shine_AdHoc/Output/ActiveDBbreakup/Function/Function_City.csv' DBMS = CSV REPLACE; RUN;

PROC SORT DATA = activeDB; BY sa_Function TotalExperience; RUN;
PROC SUMMARY DATA = activeDB; BY sa_Function TotalExperience;OUTPUT OUT = faexptemp(DROP = _TYPE_ RENAME = (_FREQ_ = Total_Candidates) RENAME = (sa_Function = Function));RUN;

DATA faexptemp;
        SET faexptemp;
        IF Function='' THEN Function='NA';
        IF TotalExperience='' THEN TotalExperience='NA';

PROC EXPORT DATA = faexptemp OUTFILE = '/data/Shine/Shine_AdHoc/Output/ActiveDBbreakup/Function/Function_Experience.csv' DBMS = CSV REPLACE; RUN;

PROC SORT DATA = activeDB; BY sa_Salary TotalExperience;RUN;
PROC SUMMARY DATA = activeDB; BY sa_Salary TotalExperience;OUTPUT OUT = salexptemp(DROP = _TYPE_ RENAME = (_FREQ_ = Total_Candidates) RENAME = (sa_Salary = Salary)); RUN;

DATA salexptemp;
        SET salexptemp;
        IF Salary='' THEN Salary='NA';
        IF TotalExperience='' THEN TotalExperience='NA';
RUN;

PROC EXPORT DATA = salexptemp OUTFILE = '/data/Shine/Shine_AdHoc/Output/ActiveDBbreakup/Experience/Experience_State.csv' DBMS = CSV REPLACE; RUN;

PROC SORT DATA = activeDB; BY TotalExperience sa_City;RUN;
PROC SUMMARY DATA = activeDB; BY TotalExperience sa_City;OUTPUT OUT = cityexptemp(DROP = _TYPE_ RENAME = (_FREQ_ = Total_Candidates) RENAME = (sa_City = City));RUN;

DATA cityexptemp;
        SET cityexptemp;
        IF City='' THEN City='NA';
        IF TotalExperience='' THEN TotalExperience='NA';
RUN;

PROC EXPORT DATA = cityexptemp OUTFILE = '/data/Shine/Shine_AdHoc/Output/ActiveDBbreakup/Experience/Experience_City.csv' DBMS = CSV REPLACE; RUN;

PROC SORT DATA = activeDB; BY TotalExperience sa_State;RUN;
PROC SUMMARY DATA = activeDB; BY TotalExperience sa_State;OUTPUT OUT = expstatetemp(DROP = _TYPE_ RENAME = (_FREQ_ = Total_Candidates) RENAME = (sa_State = State));RUN;

DATA expstatetemp;
        SET expstatetemp;
        IF TotalExperience='' THEN TotalExperience='NA';
	IF State='' THEN State='NA';
RUN;

PROC EXPORT DATA = expstatetemp OUTFILE = '/data/Shine/Shine_AdHoc/Output/ActiveDBbreakup/Experience/Experience_State.csv' DBMS = CSV REPLACE; RUN;
