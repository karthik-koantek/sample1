MERGE dbo.ValidationStatus t
USING
(
	SELECT 1 AS ValidationStatusKey, 'Passed' AS ValidationStatus
	UNION
	SELECT 2, 'Object not found'
	UNION
	SELECT 3, 'Columns do not match expected'
	UNION
	SELECT 4, 'Recent Data Validation failed'
	UNION
	SELECT 5, 'Query failed'
) s ON t.ValidationStatusKey = s.ValidationStatusKey
WHEN MATCHED THEN UPDATE SET
 t.ValidationStatus = s.ValidationStatus
WHEN NOT MATCHED THEN INSERT
(
	 ValidationStatusKey, ValidationStatus
)
VALUES
(
	 s.ValidationStatusKey, s.ValidationStatus
);