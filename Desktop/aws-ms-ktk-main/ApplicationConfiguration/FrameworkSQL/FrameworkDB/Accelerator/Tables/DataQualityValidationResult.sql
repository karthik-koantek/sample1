CREATE TABLE [dbo].[DataQualityValidationResult]
(
	 DataQualityValidationResultId BIGINT NOT NULL IDENTITY(1,1)
	,StepLogGuid UNIQUEIDENTIFIER NOT NULL
	,BatchId UNIQUEIDENTIFIER NOT NULL
	,ExpectationSuiteName VARCHAR(200) NOT NULL
	,ExpectationsVersion VARCHAR(30) NOT NULL
	,ValidationTime VARCHAR(30) NOT NULL
	,RunName VARCHAR(50) NOT NULL CONSTRAINT DF_DataQualityValidationResult_RunName DEFAULT('')
	,RunTime DATETIME2 NOT NULL CONSTRAINT DF_DataQualityValidationResult_RunTime DEFAULT(SYSDATETIME())
	,EvaluatedExpectations SMALLINT NOT NULL CONSTRAINT DF_DataQualityValidationResult_EvaluatedExpectations DEFAULT(0)
	,SuccessPercent NUMERIC(5,2) NOT NULL CONSTRAINT DF_DataQualityValidationResult_SuccessPercent DEFAULT(0.00)
	,SuccessfulExpectations SMALLINT NOT NULL CONSTRAINT DF_DataQualityValidationResult_SuccessfulExpectations DEFAULT(0)
	,UnsuccessfulExpectations SMALLINT NOT NULL CONSTRAINT DF_DataQualityValidationResult_UnsuccessfulExpectations DEFAULT(0)
)
GO
ALTER TABLE dbo.DataQualityValidationResult ADD CONSTRAINT PK_dbo_DataQualityValidationResult PRIMARY KEY CLUSTERED(DataQualityValidationResultId);
GO
CREATE UNIQUE NONCLUSTERED INDEX UIX_dbo_DataQualityValidationResult_StepLogGuid ON dbo.DataQualityValidationResult(StepLogGuid) WITH (IGNORE_DUP_KEY = ON);
GO
CREATE UNIQUE NONCLUSTERED INDEX UIX_dbo_DataQualityValidationResult_BatchId ON dbo.DataQualityValidationResult(BatchId) WITH (IGNORE_DUP_KEY = ON);
GO
