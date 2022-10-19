CREATE TABLE [dbo].[DataQualityValidationResultDetail]
(
	 DataQualityValidationResultDetailId BIGINT NOT NULL IDENTITY(1,1)
	,BatchId UNIQUEIDENTIFIER NOT NULL
	,Success BIT NOT NULL CONSTRAINT DF_dbo_DataQualityValidationResultDetail_Success DEFAULT(0)
	,ExpectationType VARCHAR(200) NOT NULL
	,ExceptionMessage VARCHAR(MAX) NULL
	,ExceptionTraceback VARCHAR(MAX) NULL
	,RaisedException BIT NOT NULL CONSTRAINT DF_dbo_DataQualityValidationResultDetail_RaisedException DEFAULT(0)
	,KwargsColumn VARCHAR(500) NOT NULL CONSTRAINT DF_dbo_DataQualityValidationResultDetail_KwargsColumn DEFAULT('')
	,KwargsColumnList VARCHAR(4000) NOT NULL CONSTRAINT DF_dbo_DataQualityValidationResultDetail_KwargsColumnList DEFAULT('')
	,KwargsMaxValue VARCHAR(20) NULL
	,KwargsMinValue VARCHAR(20) NULL
	,KwargsMostly VARCHAR(20) NULL
	,KwargsRegex VARCHAR(20) NULL
	,KwargsResultFormat VARCHAR(20) NOT NULL CONSTRAINT DF_dbo_DataQualityValidationResultDetail_KwargsResultFormat DEFAULT('BASIC')
	,KwargsTypeList VARCHAR(MAX) NULL
	,KwargsValueSet VARCHAR(MAX) NULL
	,ResultMissingPercent NUMERIC(5,2) NOT NULL CONSTRAINT DF_dbo_DataQualityValidationResultDetail_ResultMissingPercent DEFAULT(0.00)
	,ResultObservedValue VARCHAR(MAX) NULL
	,ResultPartialUnexpectedList VARCHAR(MAX) NULL
	,ResultUnexpectedCount BIGINT NOT NULL CONSTRAINT DF_dbo_DataQualityValidationResultDetail_ResultPartialUnexpectedCount DEFAULT(0)
	,ResultUnexpectedPercent NUMERIC(5,2) NOT NULL CONSTRAINT DF_dbo_DataQualityValidationResultDetail_ResultUnexpectedPercent DEFAULT(0.00)
	,ResultUnexpectedPercentNonMissing NUMERIC(5,2) NOT NULL CONSTRAINT DF_dbo_DataQualityValidationResultDetail_ResultUnexpectedPercentNonMissing DEFAULT(0.00)
	,ResultUnexpectedPercentDouble NUMERIC(5,2) NOT NULL CONSTRAINT DF_dbo_DataQualityValidationResultDetail_ResultUnexpectedPercentDouble DEFAULT(0.00)
	,ResultUnexpectedPercentTotal NUMERIC(5,2) NOT NULL CONSTRAINT DF_dbo_DataQualityValidationResultDetail_ResultUnexpectedPercentTotal DEFAULT(0.00)
)
GO
ALTER TABLE dbo.DataQualityValidationResultDetail ADD CONSTRAINT PK_dbo_DataQualityValidationResultDetail PRIMARY KEY CLUSTERED(DataQualityValidationResultDetailId);
GO
CREATE UNIQUE NONCLUSTERED INDEX UIX_dbo_DataQualityValidationResultDetail ON dbo.DataQualityValidationResultDetail(BatchId, ExpectationType, KwargsColumn, KwargsColumnList) WITH (IGNORE_DUP_KEY = ON);
GO
ALTER TABLE dbo.DataQualityValidationResultDetail ADD CONSTRAINT FK_DataQualityValidationResultDetail_DataQualityValidationResult FOREIGN KEY (BatchId) REFERENCES dbo.DataQualityValidationResult(BatchId);
GO