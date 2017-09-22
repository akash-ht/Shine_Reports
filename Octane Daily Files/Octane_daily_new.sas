OPTIONS VALIDVARNAME = ANY;

libname ConsLib "/data/Analytics/Utils/consolidatedDB/Model/SASTemp";
libname sasTemp "/data/Analytics/Utils/MarketingReports/Model/SASTemp";
libname FlatLib '/data/Analytics/Utils/dataTransfer/Model/SASTemp/CandidateDataSets';
libname FIncLib '/data/Analytics/Utils/dataTransfer/Model/SASTemp/CandidateDataSets/Incrementals';
libname CQSLib '/data/Shine/CQS/Model/SASTemp';

option mprint;  option spool;  
%include '/data/Analytics/Utils/MarketingReports/Model/SASCode/DateConversionMacro_v3.sas';

%let dayBefore = -1;

DATA totaldb(DROP = fa_BounceStatus Exp_yrs sa_gender fa_EmailAlertStatus);
        SET ConsLib.consolidatedDB(KEEP = sa_Email sa_FirstName sa_LastName sa_CellPhone sa_City sa_State sa_Status da_RegistrationStartDate 
		da_RegistrationEndDate da_LastLogin da_LastModified da_LastAppliedDate da_LastCellPhoneVerifiedDate da_LastCVUpdateDate sa_SubFunction 
		sa_Function sa_Industry sa_lastCompany sa_lastEducationalQualification sa_Specialization sa_Stream sa_UserId sa_Salary sa_JobTitle
		sa_lastInstitute fa_IsEmailVerified No_Skills fa_BounceStatus sa_Zone sa_Country sa_gender fa_IsCellPhoneVerified fa_SMSAlertFlag 
		fa_EmailAlertStatus TotalExperience Experience_Month StartVendorId EndVendorId
		RENAME = (
		sa_Email = emailid  
		sa_FirstName = first_name
		sa_LastName = last_name
		sa_CellPhone = mobile
		sa_City = City
		sa_State = State
		sa_Status = Status
		da_RegistrationStartDate = RegistrationStartDate
		da_RegistrationEndDate = RegistrationEndDate
		da_LastLogin = LastLogin
		da_LastModified = LastModified
		da_LastAppliedDate = LastAppliedDate
		da_LastCellPhoneVerifiedDate = CellPhoneVerifiedDate
		da_LastCVUpdateDate = LastCVUpdateDate
		sa_SubFunction = SubFunction
		sa_Function = Function
		sa_Industry = Industry
		sa_lastCompany = CompanyName
		sa_lastEducationalQualification = EducationQualification
		sa_Specialization = Specialization
		sa_Stream = Stream
		sa_UserId = UserId
		sa_Salary = Salary
		sa_JobTitle = JobTitle
		sa_lastInstitute = Institute
		fa_IsEmailVerified = IsEmailVerified
		No_Skills = num_skills
		));
		IF fa_BounceStatus = 'N' AND DATEPART(LastLogin)>= INTNX('day',Today(),-1);
		
		IF sa_City = 'NA' THEN DO;
			sa_City = sa_Country;
			sa_State = sa_Zone;
		END;

		IF fa_IsEmailVerified = 'Y' THEN fa_IsEmailVerified = '1';
		ELSE IF fa_IsEmailVerified = 'N' THEN fa_IsEmailVerified = '0';
		
		IF sa_gender = 'M' THEN Gender = 1;
        ELSE IF sa_gender = 'F' THEN Gender = 2;
        ELSE IF sa_gender = 'Not Set' THEN Gender = 0;

        IF fa_IsCellPhoneVerified = 'Y' THEN IsCellPhoneVerified = 1;
        ELSE IF fa_IsCellPhoneVerified = 'N' THEN IsCellPhoneVerified = 0;
        ELSE IF fa_IsCellPhoneVerified = 'Not Set' THEN IsCellPhoneVerified = .;

        IF fa_SMSAlertFlag = 'Y' THEN IsReceiveSMS = 1;
        ELSE IF fa_SMSAlertFlag = 'N' THEN IsReceiveSMS = 0;
        ELSE IF fa_SMSAlertFlag = 'Not Set' THEN IsReceiveSMS = .;

        IF fa_EmailAlertStatus = 'NO_JA,NO_OtherPdt' THEN NotificationFrequency = 0;
        ELSE IF fa_EmailAlertStatus = 'Yes_JA,Yes_OtherPdt' THEN NotificationFrequency = 1;
        ELSE IF fa_EmailAlertStatus = 'Yes_JA,NO_OtherPdt' THEN NotificationFrequency = 2;
        ELSE IF fa_EmailAlertStatus = 'NO_JA,Yes_OtherPdt' THEN NotificationFrequency = 3;
        ELSE IF fa_EmailAlertStatus = 'Not Set' THEN NotificationFrequency = .;
		
		
		Exp_yrs = SCAN(TotalExperience,1,' ');
		IF Exp_yrs = '>' THEN Exp_yrs = 25;
		ELSE IF Exp_yrs = '<1' THEN Exp_yrs = 0;
		
		_Seg_Totalexperience = Exp_yrs + (Experience_Month/12);
		_Seg_Totalexperience = ROUND(_Seg_Totalexperience, .1);
		
		
		IF Salary = 'Rs 0 / Yr' 
			THEN 
				DO	
					_Seg_Salary_Upper = 0;
					_Seg_Salary_Lower = 0;
				END;
		ELSE IF Salary = 'Rs 75 Lakh - 1 Crore / Yr'		
			THEN
				DO
					_Seg_Salary_Lower = 75;
					_Seg_Salary_Upper = 100;
				END;
				
		ELSE IF SUBSTR(Salary,1,1) = '<' 
		THEN 
			DO
				_Seg_Salary_Lower = 0;
				_Seg_Salary_Upper = 0.5;
			END;
		ELSE IF SUBSTR(Salary,1,1) = '>' 
			THEN 
				DO 
					_Seg_Salary_Lower = 100;
					_Seg_Salary_Upper = 100;
				END;
			ELSE 
				DO
				_Seg_Salary_Lower = SCAN(Salary,2,' ');
				_Seg_Salary_Upper = SCAN(Salary,4,' ');
				END;
		IF _Seg_Salary_Lower = '50000' THEN _Seg_Salary_Lower = 0.5;		
		
RUN;

PROC SORT DATA = totaldb;BY UserId;RUN;
PROC SORT DATA = CQSLib.CandidateDB;BY UserId;RUN;

DATA INPUT;
	MERGE totaldb(IN=A) CQSLib.CandidateDB(IN =B KEEP = UserId S);
	BY UserId;
	IF A;
RUN;

PROC SORT DATA = ConsLib.CandidatesToken; BY Email; RUN;
PROC SORT DATA = INPUT; BY emailid; RUN;

DATA OUT1(
	RENAME = (
	S = CQS
	RegistrationDate = 'RegistrationDate||d.m.Y'n
	CompletionDate = 'CompletionDate||d.m.Y'n
	LastLoginDate = 'LastLoginDate||d.m.Y'n
	LastUpdatedDate = 'LastUpdatedDate||d.m.Y'n
	LatestAppliedJobDate = 'LatestAppliedJobDate||d.m.Y'n
	/*CellPhoneVerificationDate = 'CellPhoneVerifiedDate||d.m.Y'n*/
	LastCVUpdationDate = 'CVUpdateDate||d.m.Y'n
	));
	RETAIN emailid first_name last_name mobile Gender City State Status RegistrationDate CompletionDate LastLoginDate LastUpdatedDate 
	LatestAppliedJobDate _Seg_Totalexperience _Seg_Salary_Lower _Seg_Salary_Upper SubFunction Function Industry CompanyName EducationQualification 
	Specialization Stream IsReceiveSMS AutoLoginToken UserId StartVendorId EndVendorId TotalExperience Experience_Month Salary JobTitle Institute 
	IsEmailVerified IsCellPhoneVerified NotificationFrequency CellPhoneVerificationDate S LastCVUpdationDate num_skills;
	MERGE INPUT(IN = A) ConsLib.CandidatesToken(RENAME = (Email = emailid));
	BY emailid;
	IF A;
	IF emailid ~= '';
	%global RegistrationDate CompletionDate LastLoginDate LastUpdatedDate LatestAppliedJobDate LastCellPhoneVerificationDate LastCVUpdationDate;
	%DateConversion2(RegistrationStartDate, RegistrationDate);
	%DateConversion2(RegistrationEndDate, CompletionDate);
	%DateConversion2(LastLogin, LastLoginDate);
	%DateConversion2(LastModified, LastUpdatedDate);
	%DateConversion2(LastAppliedDate, LatestAppliedJobDate);
	%DateConversion2(CellPhoneVerifiedDate, CellPhoneVerificationDate);
	%DateConversion2(LastCVUpdateDate, LastCVUpdationDate);
	S = ROUND(S, .1);
	IF RegistrationStartDate = . THEN RegistrationDate = '';
	IF RegistrationEndDate = . THEN CompletionDate = '';
	IF LastLogin = . THEN LastLoginDate = '';
	IF LastModified = . THEN LastUpdatedDate = '';
	IF LastAppliedDate = . THEN LatestAppliedJobDate = '';
	IF CellPhoneVerifiedDate = . THEN CellPhoneVerificationDate = '';
	IF LastCVUpdateDate = . THEN LastCVUpdationDate = '';
	DROP RegistrationStartDate RegistrationEndDate LastLogin LastModified LastAppliedDate CellPhoneVerifiedDate LastCVUpdateDate 
	fa_IsCellPhoneVerified fa_SMSAlertFlag sa_Zone	sa_Country sa_City sa_State fa_IsEmailVerified ;
RUN;

PROC EXPORT DATA = OUT1 OUTFILE = '/data/Analytics/Utils/MarketingReports/Output/Octane/Shine_Data.csv' DBMS = csv REPLACE;RUN;

DATA bounces(DROP = da_BOUpdateDate fa_BounceStatus fa_EmailAlertStatus);
	SET ConsLib.consolidatedDB(KEEP = sa_Email da_BOUpdateDate fa_BounceStatus fa_EmailAlertStatus);
	/*IF fa_BounceStatus = 'Y' AND DATEPART(da_BOUpdateDate)>= INTNX('day',Today(),-1); */
	IF fa_BounceStatus = 'Y' AND DATEPART(da_BOUpdateDate)>= INTNX('day',Today(),-1) AND (fa_EmailAlertStatus = 'NO_JA,NO_OtherPdt' OR fa_EmailAlertStatus = 'Yes_JA,NO_OtherPdt' OR fa_EmailAlertStatus = 'NO_JA,Yes_OtherPdt');
RUN;

PROC EXPORT DATA = bounces OUTFILE = '/data/Analytics/Utils/MarketingReports/Output/Octane/bounces.csv' DBMS = csv REPLACE;RUN;

