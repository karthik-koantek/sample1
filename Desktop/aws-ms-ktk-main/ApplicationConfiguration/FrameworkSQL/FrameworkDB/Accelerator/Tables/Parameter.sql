CREATE TABLE [dbo].[Parameter]
(
	 ParameterKey BIGINT NOT NULL IDENTITY(1,1)
	,StepKey BIGINT NOT NULL
	,ParameterName VARCHAR(255) NOT NULL
	,ParameterValue VARCHAR(MAX) NOT NULL CONSTRAINT DF_dbo_Parameter_ParameterValue DEFAULT('')
	,CreatedDate DATETIME2 NOT NULL CONSTRAINT DF_dbo_Parameter_CreatedDate DEFAULT(SYSDATETIME())
	,ModifiedDate DATETIME2 NULL
);
GO
ALTER TABLE dbo.Parameter ADD CONSTRAINT PK_CIX_dbo_Parameter PRIMARY KEY CLUSTERED (ParameterKey);
GO
CREATE UNIQUE NONCLUSTERED INDEX UIX_dbo_Parameter_StepKey_ParameterName ON dbo.Parameter(StepKey, ParameterName);
GO
ALTER TABLE dbo.Parameter ADD CONSTRAINT FK_Parameter_Step FOREIGN KEY (StepKey) REFERENCES dbo.Step(StepKey);
GO
