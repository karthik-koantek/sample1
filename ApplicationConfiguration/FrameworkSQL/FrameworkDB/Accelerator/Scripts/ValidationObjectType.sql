MERGE dbo.ValidationObjectType t
USING
(
	SELECT 1 AS ValidationObjectTypeKey, 'Table' AS ValidationObjectType
	UNION
	SELECT 2, 'View'
	UNION
	SELECT 3, 'Query'
) s ON t.ValidationObjectTypeKey = s.ValidationObjectTypeKey
WHEN MATCHED THEN UPDATE SET
 t.ValidationObjectType = s.ValidationObjectType
WHEN NOT MATCHED THEN INSERT
(
	 ValidationObjectTypeKey, ValidationObjectType
)
VALUES
(
	 s.ValidationObjectTypeKey, s.ValidationObjectType
);