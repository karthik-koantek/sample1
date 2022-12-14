CREATE TABLE [dbo].[Step]
(
	 StepKey BIGINT NOT NULL IDENTITY(1,1)
	,StepName VARCHAR(255) NOT NULL
	,JobKey BIGINT NOT NULL
	,StepOrder INT NOT NULL CONSTRAINT DF_dbo_Step_StepOrder DEFAULT(10)
	,IsActive BIT NOT NULL CONSTRAINT DF_dbo_Step_IsActive DEFAULT(1)
	,IsRestart BIT NOT NULL CONSTRAINT DF_dbo_Step_IsRestart DEFAULT(1)
	,CreatedDate DATETIME2 NOT NULL CONSTRAINT DF_dbo_Step_CreatedDate DEFAULT(SYSDATETIME())
	,ModifiedDate DATETIME2 NULL
);
GO
ALTER TABLE dbo.Step ADD CONSTRAINT PK_CIX_dbo_Step PRIMARY KEY CLUSTERED(StepKey);
GO
CREATE UNIQUE NONCLUSTERED INDEX UIX_dbo_Step_JobKey_StepName ON dbo.Step(JobKey, StepName);
GO
ALTER TABLE dbo.Step ADD CONSTRAINT FK_Step_Job FOREIGN KEY (JobKey) REFERENCES dbo.Job(JobKey);
GO
