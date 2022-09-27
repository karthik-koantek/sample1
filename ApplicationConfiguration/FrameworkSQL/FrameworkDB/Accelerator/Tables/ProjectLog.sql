CREATE TABLE [dbo].[ProjectLog]
(
     ProjectLogKey BIGINT NOT NULL IDENTITY(1,1)
	,ProjectLogGuid UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_dbo_ProjectLog_ProjectLogGuid DEFAULT(NEWID())
    ,ProjectKey SMALLINT NULL
    ,StartDateTime DATETIME2 NOT NULL CONSTRAINT DF_dbo_ProjectLog_StartDateTime DEFAULT(SYSDATETIME())
    ,EndDateTime DATETIME2 NULL
    ,LogStatusKey TINYINT NOT NULL
    ,Parameters VARCHAR(MAX) NULL
	,Context VARCHAR(MAX) NULL
	,Error VARCHAR(MAX) NULL
);
GO
ALTER TABLE dbo.ProjectLog ADD CONSTRAINT PK_CIX_dbo_ProjectLog PRIMARY KEY CLUSTERED (ProjectLogKey);
GO
CREATE UNIQUE NONCLUSTERED INDEX UIX_dbo_ProjectLog_ProjectLogGuid ON dbo.ProjectLog(ProjectLogGuid);
GO
CREATE NONCLUSTERED INDEX IX_dbo_ProjectLog_ProjectKey ON dbo.ProjectLog(ProjectKey);
GO
CREATE NONCLUSTERED INDEX IX_dbo_ProjectLog_LogStatusKey ON dbo.ProjectLog(LogStatusKey);
GO
--ALTER TABLE dbo.ProjectLog ADD CONSTRAINT FK_ProjectLog_Project FOREIGN KEY (ProjectKey) REFERENCES dbo.Project(ProjectKey);
GO
ALTER TABLE dbo.ProjectLog ADD CONSTRAINT FK_ProjectLog_LogStatus FOREIGN KEY (LogStatusKey) REFERENCES dbo.LogStatus(LogStatusKey);
GO
