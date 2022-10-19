CREATE TABLE [dbo].[LogStatus]
(
     LogStatusKey TINYINT NOT NULL
    ,LogStatus VARCHAR(10) NOT NULL
);
GO
ALTER TABLE dbo.LogStatus ADD CONSTRAINT PK_CIX_dbo_LogStatus PRIMARY KEY CLUSTERED (LogStatusKey);
GO
CREATE UNIQUE NONCLUSTERED INDEX UIX_dbo_LogStatus_LogStatus ON dbo.LogStatus (LogStatus);
GO
