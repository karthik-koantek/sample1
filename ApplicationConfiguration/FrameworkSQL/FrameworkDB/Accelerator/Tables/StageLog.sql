CREATE TABLE [dbo].[StageLog]
(
	 StageLogKey BIGINT NOT NULL IDENTITY(1,1)
	,StageLogGuid UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_dbo_StageLog_StageLogGuid DEFAULT(NEWID())
	,SystemLogGuid UNIQUEIDENTIFIER NULL
	,StageKey INT NULL
	,StartDateTime DATETIME2 NOT NULL CONSTRAINT DF_dbo_StageLog_StartDateTime DEFAULT(SYSDATETIME())
	,EndDateTime DATETIME2 NULL
	,LogStatusKey TINYINT NOT NULL
	,Parameters VARCHAR(MAX) NULL
	,Context VARCHAR(MAX) NULL
	,Error VARCHAR(MAX) NULL
);
GO
ALTER TABLE dbo.StageLog ADD CONSTRAINT PK_CIX_dbo_StageLog PRIMARY KEY CLUSTERED(StageLogKey);
GO
CREATE UNIQUE NONCLUSTERED INDEX UIX_dbo_StageLog_StageLogGuid ON dbo.StageLog(StageLogGuid);
GO
CREATE NONCLUSTERED INDEX IX_dbo_StageLog_SystemLogGuid ON dbo.StageLog(SystemLogGuid);
GO
CREATE NONCLUSTERED INDEX IX_dbo_StageLog_StageKey ON dbo.StageLog(StageKey);
GO
CREATE NONCLUSTERED INDEX IX_dbo_StageLog_LogStatusKey ON dbo.StageLog(LogStatusKey);
GO
ALTER TABLE dbo.StageLog ADD CONSTRAINT FK_StageLog_SystemLog FOREIGN KEY (SystemLogGuid) REFERENCES dbo.SystemLog(SystemLogGuid);
GO
--ALTER TABLE dbo.StageLog ADD CONSTRAINT FK_StageLog_Stage FOREIGN KEY (StageKey) REFERENCES dbo.Stage(StageKey);
GO
ALTER TABLE dbo.StageLog ADD CONSTRAINT FK_StageLog_LogStatus FOREIGN KEY (LogStatusKey) REFERENCES dbo.LogStatus(LogStatusKey);
GO

