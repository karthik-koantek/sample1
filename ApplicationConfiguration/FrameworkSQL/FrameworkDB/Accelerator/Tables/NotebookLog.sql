CREATE TABLE dbo.NotebookLog
(
     NotebookLogKey BIGINT NOT NULL IDENTITY(1,1)
    ,NotebookLogGuid UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_dbo_NotebookLog_NotebookLogGuid DEFAULT(NEWID())
    ,StepLogGuid UNIQUEIDENTIFIER NULL
    ,StepKey BIGINT NULL
    ,StartDateTime DATETIME2 NOT NULL CONSTRAINT DF_dbo_NotebookLog_StartDateTime DEFAULT(SYSDATETIME())
    ,EndDateTime DATETIME2 NULL
    ,LogStatusKey TINYINT NOT NULL
    ,RowsAffected BIGINT NULL
    ,Parameters VARCHAR(MAX) NULL
    ,Context VARCHAR(MAX) NULL
    ,Error VARCHAR(MAX) NULL
);
GO
ALTER TABLE dbo.NotebookLog ADD CONSTRAINT PK_CIX_dbo_NotebookLog PRIMARY KEY CLUSTERED(NotebookLogKey);
GO
CREATE UNIQUE NONCLUSTERED INDEX UIX_dbo_NotebookLog_NotebookLogGuid ON dbo.NotebookLog(NotebookLogGuid);
GO
CREATE NONCLUSTERED INDEX IX_dbo_NotebookLog_StepLogGuid ON dbo.NotebookLog(StepLogGuid);
GO
CREATE NONCLUSTERED INDEX IX_dbo_NotebookLog_StepKey ON dbo.NotebookLog(StepKey)
GO
CREATE NONCLUSTERED INDEX IX_dbo_NotebookLog_LogStatusKey ON dbo.NotebookLog(LogStatusKey);
GO
ALTER TABLE dbo.NotebookLog ADD CONSTRAINT FK_NotebookLog_StepLog FOREIGN KEY (StepLogGuid) REFERENCES dbo.StepLog(StepLogGuid);
GO
--ALTER TABLE dbo.NotebookLog ADD CONSTRAINT FK_NotebookLog_Step FOREIGN KEY (StepKey) REFERENCES dbo.Step(StepKey);
GO
ALTER TABLE dbo.NotebookLog ADD CONSTRAINT FK_NotebookLog_LogStatus FOREIGN KEY (LogStatusKey) REFERENCES dbo.LogStatus(LogStatusKey);
GO
