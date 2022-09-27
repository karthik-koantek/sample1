MERGE dbo.DatabaseCatalog t
USING
(
	SELECT 1 AS DatabaseCatalogKey, 'bronze' AS DatabaseCatalog
	UNION
	SELECT 2, 'silvergeneral'
	UNION
	SELECT 3, 'silverprotected'
	UNION
	SELECT 4, 'goldgeneral'
	UNION
	SELECT 5, 'goldprotected'
	UNION
	SELECT 6, 'platinum'
	UNION
	SELECT 7, 'sandbox'
	UNION
	SELECT 8, 'default'
	UNION
	SELECT 9, 'archive'
) s ON t.DatabaseCatalogKey = s.DatabaseCatalogKey
WHEN MATCHED THEN UPDATE SET
 t.DatabaseCatalog = s.DatabaseCatalog
WHEN NOT MATCHED THEN INSERT
(
	 DatabaseCatalogKey, DatabaseCatalog
)
VALUES
(
	 s.DatabaseCatalogKey, s.DatabaseCatalog
);