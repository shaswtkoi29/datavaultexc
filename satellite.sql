
CREATE PROCEDURE [ETL].[PopulateSATStudentAdmission]

AS

SET NOCOUNT ON;


DECLARE @RS        varchar(150)  --RecordSource
DECLARE @LDTS		datetime 
DECLARE @LEDTS		datetime
 
SET @RS    = 'Housing.csv'
SET @LDTS  = GETDATE()
SET @LEDTS = NULL
 

BEGIN TRY
Begin Transaction
 
  
    
IF OBJECT_ID('tempdb..#SATStudentAdmission') is not null
BEGIN
      DROP TABLE #SATStudentAdmission
END
-- Declare temp table.
Create Table  #SATStudentAdmission
(
	StudentAdmissionSK bigint
	  , AdmissionConfirmedDate CHAR(11)
	  , AdmissionTerm VARCHAR(2)
	  , AdmissionStatus CHAR(3)
	  , EntryType CHAR(8)
) 
/*Insert changed records into temp table*/
INSERT INTO  #SATStudentAdmission 
(
	StudentAdmissionSK
	  , AdmissionConfirmedDate
	  , AdmissionTerm
	  , AdmissionStatus
	  , EntryType
)

SELECT
	StudentAdmissionSK
	  , AdmissionConfirmedDate
	  , AdmissionTerm
	  , AdmissionStatus
	  , EntryType

FROM
(
     MERGE SATStudentAdmission AS Target     --Target: Satellite
     USING
     (
          -- Query distinct set of attributes from source (stage)
          -- includes lookup of business key by left outer join referenced hub/link
          SELECT DISTINCT 
			Hub.StudentAdmissionSK
	  ,		SD.AdmissionConfirmedDate
	  ,	 MAX(SD.AdmissionTerm) OVER (PARTITION BY Hub.StudentID) AS 'AdmissionTerm'
	  --,		SD.AdmissionTerm
	  --,		SD.AdmissionStatus
	  ,  MAX(SD.AdmissionStatus) OVER (PARTITION BY Hub.StudentID) AS 'AdmissionStatus'
	  
	  ,		SD.EntryType
		  FROM DWStaging.dbo.StudentData SD
			LEFT JOIN HUBStudentAdmission Hub
			on SD.StudentID = Hub.StudentID
		
		  WHERE StudentAdmissionSK is not null

     ) AS Source
     ON Target.StudentAdmissionSK = Source.StudentAdmissionSK         --Identify Columns by Hub/Link Surrogate Key
     AND LEDTS is null                         --and only merge against current records in the target
     --when record already exists in satellite and an attribute value changed
     WHEN MATCHED AND
     (
		       ISNULL(Target.AdmissionConfirmedDate,'') <> ISNULL(Source.AdmissionConfirmedDate,'')	
			   OR ISNULL(Target.AdmissionTerm,'') <> ISNULL(Source.AdmissionTerm,'')	
			   OR ISNULL(Target.AdmissionStatus,'') <> ISNULL(Source.AdmissionStatus,'')
			   OR ISNULL(Target.EntryType,'') <> ISNULL(Source.EntryType,'')
     )
     -- then outdate the existing record
     THEN UPDATE SET
          LEDTS = @LDTS
     -- when record not exists in satellite, insert the new record
     WHEN NOT MATCHED BY TARGET
     THEN INSERT
     (
         StudentAdmissionSK
      , LDTS
	  , AdmissionConfirmedDate
	  , AdmissionTerm
	  , AdmissionStatus
	  , EntryType
      , LEDTS
      , RS
     )
     VALUES
     (
          Source.StudentAdmissionSK
		  , @LDTS
		  , Source.AdmissionConfirmedDate
		  , Source.AdmissionTerm
		  , Source.AdmissionStatus
		  , Source.EntryType
		  ,@LEDTS
		  ,@RS
     )
     -- Output changed records
     OUTPUT
          $action AS Action
          ,Source.*
) AS MergeOutput
WHERE MergeOutput.Action = 'UPDATE'
AND StudentAdmissionSK IS NOT NULL;
 INSERT INTO SATStudentAdmission
(
          StudentAdmissionSK
		  , LDTS
		  , AdmissionConfirmedDate
		  , AdmissionTerm
		  , AdmissionStatus
		  , EntryType
		  ,LEDTS
		  ,RS 
)
SELECT
          StudentAdmissionSK
		  , @LDTS
		  , AdmissionConfirmedDate
		  , AdmissionTerm
		  , AdmissionStatus
		  , EntryType
		  ,@LEDTS
		  ,@RS AS RS
FROM #SATStudentAdmission


DROP TABLE #SATStudentAdmission
Commit
     SELECT
          'Success' as ExecutionResult
     RETURN;
END TRY
 
BEGIN CATCH
 
     IF @@TRANCOUNT > 0
     ROLLBACK
 
     SELECT
          'Failure' as ExecutionResult,
          ERROR_MESSAGE() AS ErrorMessage;
     RETURN;
END CATCH
