CREATE TABLE [dbo].[AppInsightsSearchKeywords]
(
	 [SK] BIGINT NOT NULL IDENTITY(1,1)
	,[Tool] VARCHAR(20) NOT NULL
	,[ContextSessionId] VARCHAR(20) NOT NULL
	,[Date] DATE NOT NULL
	,[UserID] UNIQUEIDENTIFIER NOT NULL
	,[RoleID] UNIQUEIDENTIFIER NOT NULL
	,[SegmentID] UNIQUEIDENTIFIER NOT NULL
	,[SearchKeyword] VARCHAR(255) NOT NULL
	,[City] VARCHAR(100) NOT NULL
	,[Country] VARCHAR(100) NOT NULL
	,[Province] VARCHAR(100) NOT NULL
	,[Count] BIGINT NOT NULL CONSTRAINT DF_dbo_AppInsightsSearchKeywords DEFAULT(0)
)
GO
ALTER TABLE dbo.AppInsightsSearchKeywords ADD CONSTRAINT PK_CIX_dbo_AppInsightsSearchKeywords PRIMARY KEY CLUSTERED([SK]);
GO
CREATE NONCLUSTERED INDEX IX_dbo_AppInsightsSearchKeywords_Date ON dbo.AppInsightsSearchKeywords ([Date]);

