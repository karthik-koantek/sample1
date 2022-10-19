CREATE TABLE [dbo].[ValidationObjectType]
(
     ValidationObjectTypeKey SMALLINT NOT NULL
    ,ValidationObjectType VARCHAR(100) NOT NULL
);
GO
ALTER TABLE dbo.[ValidationObjectType] ADD CONSTRAINT PK_CIX_dbo_ValidationObjectType PRIMARY KEY CLUSTERED (ValidationObjectTypeKey);
GO
CREATE UNIQUE NONCLUSTERED INDEX UIX_dbo_ValidationObjectType_ValidationObjectType ON dbo.ValidationObjectType (ValidationObjectType);
GO
