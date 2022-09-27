CREATE TABLE [dbo].[DatabaseCatalog]
(
     DatabaseCatalogKey SMALLINT NOT NULL
    ,DatabaseCatalog VARCHAR(100) NOT NULL
);
GO
ALTER TABLE dbo.[DatabaseCatalog] ADD CONSTRAINT PK_CIX_dbo_DatabaseCatalog PRIMARY KEY CLUSTERED (DatabaseCatalogKey);
GO
CREATE UNIQUE NONCLUSTERED INDEX UIX_dbo_DatabaseCatalog_DatabaseCatalog ON dbo.DatabaseCatalog (DatabaseCatalog);
GO
