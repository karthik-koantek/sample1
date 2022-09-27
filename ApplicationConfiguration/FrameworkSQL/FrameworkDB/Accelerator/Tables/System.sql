CREATE TABLE [dbo].[System]
(
	 SystemKey INT NOT NULL IDENTITY(1,1)
	,SystemName VARCHAR(255) NOT NULL
	,SystemSecretScope VARCHAR(255) NOT NULL
	,ProjectKey SMALLINT NOT NULL
	,SystemOrder INT NOT NULL CONSTRAINT DF_dbo_System_ExternalSystemOrder DEFAULT(10)
	,IsActive BIT NOT NULL CONSTRAINT DF_dbo_System_IsActive DEFAULT(1)
	,IsRestart BIT NOT NULL CONSTRAINT DF_dbo_System_IsRestart DEFAULT(1)
	,CreatedDate DATETIME2 NOT NULL CONSTRAINT DF_dbo_System_CreatedDate DEFAULT(SYSDATETIME())
	,ModifiedDate DATETIME2 NULL
);
GO
ALTER TABLE dbo.System ADD CONSTRAINT PK_CIX_dbo_System PRIMARY KEY CLUSTERED (SystemKey);
GO
CREATE UNIQUE NONCLUSTERED INDEX UIX_dbo_System_ProjectKey_SystemName ON dbo.[System](ProjectKey, SystemName);
GO
CREATE NONCLUSTERED INDEX IX_dbo_ExternalSystem ON dbo.[System](ProjectKey);
GO
ALTER TABLE dbo.System ADD CONSTRAINT FK_System_Project FOREIGN KEY (ProjectKey) REFERENCES dbo.Project (ProjectKey);
GO


