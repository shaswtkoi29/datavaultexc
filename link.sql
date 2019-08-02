CREAT PROCEDURE [ETL].[PopulateLNKStudentTerm_AcademicProgram]
AS
SET NOCOUNT ON;

DECLARE @LDTS DATETIME = getdate()


IF OBJECT_ID('tempdb..#LNKStudentTerm_AcademicProgram') is not null
BEGIN
      DROP TABLE #LNKStudentTerm_AcademicProgram
END
-- Declare temp table.
Create Table  #LNKStudentTerm_AcademicProgram
(
	StudentTermSK int
	, AcademicProgramSK int
	, RS varchar(150)
)

INSERT INTO  #LNKStudentTerm_AcademicProgram
(
       StudentTermSK
	   , AcademicProgramSK
       , RS
)
SELECT DISTINCT  h.StudentTermSK
	, prog.AcademicProgramSK
	, 'DWStaging.dbo.StudentData'  
from HUBStudentTerm h
LEFT JOIN DWStaging.dbo.StudentData stg 
	on h.StudentID = stg.StudentID 
		and h.TermCode = stg.TermCode
LEFT JOIN HUBAcademicProgram prog
	on stg.SRAcademicProgram = prog.AcademicProgramCode
WHERE prog.AcademicProgramSK is not null



-- Begin Merge.
MERGE INTO dbo.LNKStudentTerm_AcademicProgram AS tgt
USING (
       Select StudentTermSK
	   , AcademicProgramSK
       , RS
       FROM #LNKStudentTerm_AcademicProgram

       ) AS src(StudentTermSK, AcademicProgramSK, RS)
       ON tgt.StudentTermSK = src.StudentTermSK
             AND tgt.AcademicProgramSK = src.AcademicProgramSK
-- Merge Update
WHEN MATCHED
and src.RS <> tgt.RS

THEN UPDATE
SET tgt.RS = src.RS

-- Merge Insert
WHEN NOT MATCHED BY TARGET
	THEN
		INSERT (
			StudentTermSK
			, AcademicProgramSK
			, LDTS
			, RS
			)
		VALUES (
			src.StudentTermSK
			, src.AcademicProgramSK
			, @LDTS
			, src.RS
			);
	

-- Dropping temp table.
Drop Table #LNKStudentTerm_AcademicProgram
;
