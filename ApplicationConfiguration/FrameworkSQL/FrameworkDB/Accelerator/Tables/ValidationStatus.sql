CREATE TABLE [dbo].[ValidationStatus]
(
     ValidationStatusKey TINYINT NOT NULL
    ,ValidationStatus VARCHAR(200) NOT NULL
);
GO
ALTER TABLE dbo.ValidationStatus ADD CONSTRAINT PK_CIX_dbo_ValidationStatus PRIMARY KEY CLUSTERED (ValidationStatusKey);
GO
CREATE UNIQUE NONCLUSTERED INDEX UIX_dbo_ValidationStatus_ValidationStatus ON dbo.ValidationStatus (ValidationStatus);
GO
