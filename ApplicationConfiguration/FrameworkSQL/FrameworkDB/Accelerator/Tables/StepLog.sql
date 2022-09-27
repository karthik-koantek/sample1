﻿CREATE TABLE [dbo].[StepLog]
(
	 StepLogKey BIGINT NOT NULL IDENTITY(1,1)
	,StepLogGuid UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_dbo_StepLog_StepLogGuid DEFAULT(NEWID())
	,JobLogGuid UNIQUEIDENTIFIER NULL
	,StepKey BIGINT NULL
	,StartDateTime DATETIME2 NOT NULL CONSTRAINT DF_dbo_StepLog_StartDateTime DEFAULT(SYSDATETIME())
	,EndDateTime DATETIME NULL
	,LogStatusKey TINYINT NOT NULL
	,Parameters VARCHAR(MAX) NULL
	,Context VARCHAR(MAX) NULL
	,Error VARCHAR(MAX) NULL
);
GO
ALTER TABLE dbo.StepLog ADD CONSTRAINT PK_CIX_dbo_StepLog PRIMARY KEY CLUSTERED(StepLogKey);
GO
CREATE UNIQUE NONCLUSTERED INDEX UIX_dbo_StepLog_StepLogGuid ON dbo.StepLog(StepLogGuid);
GO
CREATE NONCLUSTERED INDEX IX_dbo_StepLog_JobLogGuid ON dbo.StepLog(JobLogGuid);
GO
CREATE NONCLUSTERED INDEX IX_dbo_StepLog_StepKey ON dbo.StepLog(StepKey);
GO
CREATE NONCLUSTERED INDEX IX_dbo_StepLog_LogStatusKey ON dbo.StepLog(LogStatusKey);
GO
ALTER TABLE dbo.StepLog ADD CONSTRAINT FK_StepLog_JobLog FOREIGN KEY (JobLogGuid) REFERENCES dbo.JobLog(JobLogGuid);
GO
--ALTER TABLE dbo.StepLog ADD CONSTRAINT FK_StepLog_Step FOREIGN KEY (StepKey) REFERENCES dbo.Step(StepKey);
GO
ALTER TABLE dbo.StepLog ADD CONSTRAINT FK_StepLog_LogStatus FOREIGN KEY (LogStatusKey) REFERENCES dbo.LogStatus(LogStatusKey);
GO