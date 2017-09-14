OPTION OBS = MAX;

LIBNAME consdb '/data/Analytics/Utils/consolidatedDB/Model/SASTemp';

DATA second_last_login;
	FORMAT counter BEST32. ;
	SET consdb.loginhistory (KEEP = UserId LastLogin);
	BY UserId;
	IF first.UserId then counter = 0;
	counter = counter + 1 ;
	retain counter;
	IF counter = 2 then output;
	RUN;

DATA data_cut;
SET second_last_login;
WHERE LastLogin < INTNX('day',Today(),-184) ;
RUN;

DATA lastlogin_data;
FORMAT counter BEST32. ;
SET consdb.loginhistory (KEEP = UserId LastLogin);
BY UserId;
IF first.UserId then counter = 0;
counter = counter + 1 ;
retain counter;
IF counter = 1 then output;
RUN;

DATA data_cut_lastlogin;
SET lastlogin_data;
WHERE LastLogin = INTNX('day',Today(),-1) ;
RUN;

PROC SORT DATA = data_cut; BY UserId; RUN;
PROC SORT DATA = data_cut_lastlogin; BY UserId; RUN;

DATA revivals;
MERGE data_cut(IN =A) data_cut_lastlogin(IN = B) ;
BY UserId;
IF A AND B ;
RUN;
	
DATA lastday_data;
SET consDB.consolidateddb(KEEP = sa_UserId sa_Source EndVendorId sa_City sa_Status da_LastLogin da_LastModified sa_Function sa_Industry sa_experience sa_Salary sa_State sa_gender sa_lastEducationalQualification);
IF sa_Status = 'Activated' and DATEPART(da_LastLogin) >= INTNX('day',Today(),-1) and DATEPART(da_LastModified) >= INTNX('day',Today(),-730);
run;

PROC SORT DATA = lastday_data; BY sa_UserId; RUN;

DATA revivals_profiles;
MERGE revivals(IN =A) lastday_data(IN = B RENAME =(sa_UserId = UserId)) ;
BY UserId;
IF A AND B ;
RUN;

PROC EXPORT DATA = revivals_profiles OUTFILE = '/data/Shine/Shine_AdHoc/Output/revival_profiles.csv' DBMS = CSV REPLACE; RUN;

FILENAME outbox email 'himanshu.solanki@hindustantimes.com';
DATA _NULL_;
 file outbox 
 to = ('prateek.agarwal1@hindustantimes.com')
 cc = ('himanshu.solanki@hindustantimes.com')
 Subject = 'Revival Candidate Profiles'
 attach = ('/data/Shine/Shine_AdHoc/Output/revival_profiles.csv');
 PUT "Hi Amit";
 PUT "PFA the profile of revived candidates for yesterday" ; 
 RUN;








