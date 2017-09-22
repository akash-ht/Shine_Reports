OPTIONS VALIDVARNAME = ANY;

LIBNAME consDB "/data/Analytics/Utils/consolidatedDB/Model/SASTemp";
LIBNAME myLib "/data/Shine/Shine_AdHoc/Model/SASTemp";
LIBNAME search "/data/Shine/CandidateSearchQuality/Model/SASTemp";
LIBNAME look "/data/Analytics/Utils/consolidatedDB/Model/Lookups";

DATA cold_calling_crm_emails;
    INFILE "/data/Shine/Shine_AdHoc/Output/enquire_now_everyday.csv" DLM=',' DSD LRECL = 3326 TRUNCOVER FIRSTOBS=2 ;
    INPUT email :$50. product :$200.;
RUN;

PROC SQL;
    CREATE TABLE cold_calling_crm_data as
    SELECT a.email as EmailId, b.sa_UserId as UserId, b.sa_FirstName as FirstName,b.sa_LastName as LastName, b.sa_CellPhone as ContactNumber, b.sa_City as City,
    b.sa_experience as TotExperience, b.sa_Salary as Salary, b.sa_Industry as Industry, a.product as CourseName, b.da_DateOfBirth, b.sa_Specialization, b.sa_SubFunction
    FROM cold_calling_crm_emails as a, consdb.consolidateddb as b
    WHERE a.email = b.sa_Email
    ORDER BY UserId
    ;
RUN;

DATA cold_calling_crm_data_temp;
    MERGE cold_calling_crm_data(IN=A KEEP = UserId) consDB.CandidateJobs(IN=B KEEP = UserId CandidateJobId);
    By UserId;
    IF A;
RUN;

PROC SUMMARY DATA = cold_calling_crm_data_temp; BY UserId; output out = temp1(DROP = _TYPE_ RENAME=(_FREQ_= TotalJobs));RUN;

DATA cold_calling_crm_data(DROP = sa_UserId);
    MERGE cold_calling_crm_data(IN=A) temp1(IN=B);
    BY UserId;
    IF A;
RUN; 

PROC EXPORT DATA = cold_calling_crm_data OUTFILE = '/data/Shine/Shine_AdHoc/Output/enquire_now_everyday_data.csv' DBMS = CSV REPLACE; RUN;
