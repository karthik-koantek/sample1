CREATE TABLE [dbo].[SystemLog]
(
	 SystemLogKey BIGINT NOT NULL IDENTITY(1,1)
	,SystemLogGuid UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_dbo_SystemLog_SystemLogGuid DEFAULT(NEWID())
	,ProjectLogGuid UNIQUEIDENTIFIER NULL
	,SystemKey INT NULL
	,StartDateTime DATETIME2 NOT NULL CONSTRAINT DF_dbo_SystemLog_StartDateTime DEFAULT(SYSDATETIME())
    ,EndDateTime DATETIME2 NULL
    ,LogStatusKey TINYINT NOT NULL
    ,Parameters VARCHAR(MAX) NULL
	,Context VARCHAR(MAX) NULL
	,Error VARCHAR(MAX) NULL
);
GO
ALTER TABLE dbo.SystemLog ADD CONSTRAINT PK_CIX_dbo_SystemLog PRIMARY KEY CLUSTERED (SystemLogKey);
GO
CREATE UNIQUE NONCLUSTERED INDEX UIX_dbo_SystemLog_ExternalSystemLogGuid ON dbo.SystemLog(SystemLogGuid);
GO
CREATE NONCLUSTERED INDEX IX_dbo_SystemLog_SystemKey ON dbo.SystemLog(SystemKey);
GO
CREATE NONCLUSTERED INDEX IX_dbo_SystemLog_LogStatusKey ON dbo.SystemLog(LogStatusKey);
GO
ALTER TABLE dbo.SystemLog ADD CONSTRAINT FK_SystemLog_ProjectLog FOREIGN KEY (ProjectLogGuid) REFERENCES dbo.ProjectLog(ProjectLogGuid);
GO
--ALTER TABLE dbo.SystemLog ADD CONSTRAINT FK_SystemLog_System FOREIGN KEY (SystemKey) REFERENCES dbo.[System](SystemKey);
GO
ALTER TABLE dbo.SystemLog ADD CONSTRAINT FK_SystemLog_LogStatus FOREIGN KEY (LogStatusKey) REFERENCES dbo.LogStatus(LogStatusKey);
GO
