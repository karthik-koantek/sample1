CREATE TABLE [dbo].[WindowedExtraction]
(
	 WindowedExtractionKey BIGINT NOT NULL IDENTITY(1,1)
	,StepKey BIGINT NOT NULL
	,WindowStart VARCHAR(20) NOT NULL
	,WindowEnd VARCHAR(20) NOT NULL
	,WindowOrder INT NOT NULL CONSTRAINT DF_dbo_WindowedExtraction_WindowOrder DEFAULT(10)
	,IsActive BIT NOT NULL CONSTRAINT DF_dbo_WindowedExtraction_IsActive DEFAULT(1)
	,ExtractionTimestamp DATETIME2 NULL
	,QueryZoneTimestamp DATETIME2 NULL
)
GO
ALTER TABLE dbo.WindowedExtraction ADD CONSTRAINT PK_CIX_dbo_WindowedExtraction PRIMARY KEY CLUSTERED(WindowedExtractionKey) WITH(IGNORE_DUP_KEY=ON);
GO
CREATE NONCLUSTERED INDEX IX_dbo_WindowedExtraction_ParameterKey_WindowOrder ON dbo.WindowedExtraction(StepKey, WindowOrder) INCLUDE(IsActive, WindowStart, WindowEnd);
GO
ALTER TABLE dbo.WindowedExtraction ADD CONSTRAINT FK_WindowedExtraction_Parameter FOREIGN KEY(StepKey) REFERENCES dbo.Step(StepKey);
GO

