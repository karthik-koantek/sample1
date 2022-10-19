CREATE TABLE [dbo].[ValidationLog]
(
	 ValidationLogKey BIGINT NOT NULL IDENTITY(1,1)
	,ValidationLogGuid UNIQUEIDENTIFIER NOT NULL CONSTRAINT DF_dbo_ValidationLog_ValidationLogGuid DEFAULT(NEWID())
	,StepLogGuid UNIQUEIDENTIFIER NULL
	,ValidationKey BIGINT NULL
	,ValidationStatusKey TINYINT NULL
	,Parameters VARCHAR(MAX) NULL
	,Error VARCHAR(MAX) NULL
)
GO
ALTER TABLE dbo.[ValidationLog] ADD CONSTRAINT PK_CIX_dbo_ValidationLog PRIMARY KEY CLUSTERED(ValidationLogKey);
GO
CREATE UNIQUE NONCLUSTERED INDEX UIX_dbo_ValidationLog_ValidationLogGuid ON dbo.ValidationLog(ValidationLogGuid);
GO
CREATE NONCLUSTERED INDEX IX_dbo_ValidationLog_StepLogGuid ON dbo.ValidationLog(StepLogGuid);
GO
CREATE NONCLUSTERED INDEX IX_dbo_ValidationLog_ValidationKey ON dbo.ValidationLog(ValidationKey);
GO
CREATE NONCLUSTERED INDEX IX_dbo_ValidationLog_ValidationStatusKey ON dbo.ValidationLog(ValidationStatusKey);
GO