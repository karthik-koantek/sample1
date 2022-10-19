CREATE TABLE [dbo].[Stage]
(
	 StageKey INT NOT NULL IDENTITY(1,1)
	,StageName VARCHAR(255) NOT NULL
	,SystemKey INT NOT NULL
	,IsActive BIT NOT NULL CONSTRAINT DF_dbo_Stage_IsActive DEFAULT(1)
	,IsRestart BIT NOT NULL CONSTRAINT DF_dbo_Stage_IsRestart DEFAULT(1)
	,StageOrder SMALLINT NOT NULL CONSTRAINT DF_dbo_Stage_StageOrder DEFAULT(10)
	,NumberOfThreads TINYINT NOT NULL CONSTRAINT DF_dbo_Stage_NumberOfThreads DEFAULT(1)
	,CreatedDate DATETIME2 NOT NULL CONSTRAINT DF_dbo_Stage_CreatedDate DEFAULT(SYSDATETIME())
	,ModifiedDate DATETIME2 NULL
);
GO
ALTER TABLE dbo.Stage ADD CONSTRAINT PK_CIX_dbo_Stage PRIMARY KEY CLUSTERED (StageKey);
GO
CREATE UNIQUE NONCLUSTERED INDEX UIX_dbo_Stage_SystemKey_StageName ON dbo.Stage(SystemKey, StageName);
GO
CREATE NONCLUSTERED INDEX IX_dbo_Stage_ExternalSystemKey ON dbo.Stage(SystemKey);
GO
ALTER TABLE dbo.Stage ADD CONSTRAINT FK_Stage_ExternalSystem FOREIGN KEY (SystemKey) REFERENCES dbo.[System](SystemKey);
GO

