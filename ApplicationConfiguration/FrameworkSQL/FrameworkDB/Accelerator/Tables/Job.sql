﻿CREATE TABLE [dbo].[Job]
(
	 JobKey BIGINT NOT NULL IDENTITY(1,1)
	,JobName VARCHAR(255) NOT NULL
	,StageKey INT NOT NULL
	,JobOrder INT NOT NULL CONSTRAINT DF_dbo_Job_JobOrder DEFAULT(10)
	,IsActive BIT NOT NULL CONSTRAINT DF_dbo_Job_IsActive DEFAULT(1)
	,IsRestart BIT NOT NULL CONSTRAINT DF_dbo_Job_IsRestart DEFAULT(1)
	,CreatedDate DATETIME2 NOT NULL CONSTRAINT DF_dbo_Job_CreatedDate DEFAULT(SYSDATETIME())
	,ModifiedDate DATETIME2 NULL
);
GO
ALTER TABLE dbo.Job ADD CONSTRAINT PK_CIX_dbo_Job PRIMARY KEY CLUSTERED (JobKey);
GO
CREATE UNIQUE NONCLUSTERED INDEX UIX_dbo_Job_StageKey_JobName ON dbo.Job(StageKey, JobName);
GO
CREATE NONCLUSTERED INDEX IX_dbo_Job_StageKey ON dbo.Job(StageKey);
GO
ALTER TABLE dbo.Job ADD CONSTRAINT FK_Job_Stage FOREIGN KEY (StageKey) REFERENCES dbo.Stage(StageKey);
GO