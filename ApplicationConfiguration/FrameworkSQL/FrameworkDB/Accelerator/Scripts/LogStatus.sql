MERGE dbo.LogStatus t
USING
(
	SELECT 1 AS LogStatusKey, 'Started' AS LogStatus
	UNION
	SELECT 2, 'Completed'
	UNION
	SELECT 3, 'Failed'
) s ON t.LogStatusKey = s.LogStatusKey
WHEN MATCHED THEN UPDATE SET
 t.LogStatus = s.LogStatus
WHEN NOT MATCHED THEN INSERT
(
	 LogStatusKey, LogStatus
)
VALUES
(
	 s.LogStatusKey, s.LogStatus
);