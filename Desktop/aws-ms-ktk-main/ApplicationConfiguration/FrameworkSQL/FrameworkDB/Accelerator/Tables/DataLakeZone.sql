CREATE TABLE [dbo].[DataLakeZone]
(
     DataLakeZoneKey TINYINT NOT NULL
    ,DataLakeZone VARCHAR(100) NOT NULL
);
GO
ALTER TABLE dbo.[DataLakeZone] ADD CONSTRAINT PK_CIX_dbo_DataLakeZone PRIMARY KEY CLUSTERED (DataLakeZoneKey);
GO
CREATE UNIQUE NONCLUSTERED INDEX UIX_dbo_DataLakeZone_DataLakeZone ON dbo.DataLakeZone (DataLakeZone);
GO
