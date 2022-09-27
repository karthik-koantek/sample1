CREATE TABLE [dbo].[Validation]
(
	 ValidationKey BIGINT NOT NULL IDENTITY(1,1)
	,DataLakeZoneKey TINYINT NOT NULL
	,DatabaseCatalogKey SMALLINT NULL
	,ValidationObjectTypeKey SMALLINT NOT NULL
	,ObjectName VARCHAR(400) NOT NULL CONSTRAINT DF_dbo_Validation_ObjectName DEFAULT('')
	,Query VARCHAR(MAX) NULL
	,IsActive BIT NOT NULL CONSTRAINT DF_dbo_Validation_IsActive DEFAULT(1)
	,IsRestart BIT NOT NULL CONSTRAINT DF_dbo_Validation_IsRestart DEFAULT(1)
	,CreatedDate DATETIME2 NOT NULL CONSTRAINT DF_dbo_Validation_CreatedDate DEFAULT(SYSDATETIME())
	,ModifiedDate DATETIME2 NULL
	,ExpectedColumns VARCHAR(MAX) NULL
	,ExpectedNewOrModifiedRows2Days BIGINT NOT NULL CONSTRAINT DF_dbo_Validation_ExpectedNewOrModifiedRows2Days DEFAULT(0)
	,ExpectedNewOrModifiedRows6Days BIGINT NOT NULL CONSTRAINT DF_dbo_Validation_ExpectedNewOrModifiedRows6Days DEFAULT(0)
	,ExpectedRowCount BIGINT NOT NULL CONSTRAINT DF_dbo_Validation_ExpectedRowCount DEFAULT(0)
)
GO
ALTER TABLE dbo.[Validation] ADD CONSTRAINT PK_CIX_dbo_Validation PRIMARY KEY CLUSTERED(ValidationKey);
GO
CREATE NONCLUSTERED INDEX IX_dbo_Validation_DataLakeZoneKey_ObjectName_Query ON dbo.[Validation](DataLakeZoneKey, ObjectName);
GO
ALTER TABLE dbo.[Validation] ADD CONSTRAINT FK_Validation_DataLakeZone FOREIGN KEY (DataLakeZoneKey) REFERENCES dbo.DataLakeZone(DataLakeZoneKey);
GO
ALTER TABLE dbo.[Validation] ADD CONSTRAINT FK_Validation_DatabaseCatalog FOREIGN KEY (DatabaseCatalogKey) REFERENCES dbo.DatabaseCatalog(DatabaseCatalogKey);
GO
ALTER TABLE dbo.[Validation] ADD CONSTRAINT FK_Validation_ValidationObjectType FOREIGN KEY (ValidationObjectTypeKey) REFERENCES dbo.ValidationObjectType(ValidationObjectTypeKey);
GO

