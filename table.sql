CREATE TABLE [dbo].[SATStudentAcademicPlan](
	[StudentSK]		BIGINT		NOT NULL,
    [AcademicPlanSK] INT       NOT NULL,
    [LDTS]          DATETIME     NOT NULL,
    [LEDTS]         DATETIME     NULL,
    [RS]            VARCHAR (50) NOT NULl
	CONSTRAINT [PK_SATStudentAcademicPlan] PRIMARY KEY CLUSTERED ([StudentSK] ASC,[AcademicPlanSK] ASC, [LDTS] ASC),
    CONSTRAINT [FK_SATStudentAcademicPlan_HUBAcademicPlan] FOREIGN KEY ([AcademicPlanSK]) REFERENCES [dbo].[HUBAcademicPlan] ([AcademicPlanSK]),
    CONSTRAINT [FK_SATStudentAcademicPlan_HUBStudent] FOREIGN KEY ([StudentSK]) REFERENCES [dbo].[HUBStudent]([StudentSK]),  
	CONSTRAINT [UK_SATStudentAcademicPlan] UNIQUE NONCLUSTERED ([StudentSK] ASC, [AcademicPlanSK] ASC)
);
GO
