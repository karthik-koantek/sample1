MERGE dbo.DataLakeZone t
USING
(
	SELECT 1 AS DataLakeZoneKey, 'bronze' AS DataLakeZone
	UNION
	SELECT 2, 'silver'
	UNION
	SELECT 3, 'gold'
	UNION
	SELECT 6, 'platinum'
	UNION
	SELECT 7, 'sandbox'
	UNION
	SELECT 8, 'default'
) s ON t.DataLakeZoneKey = s.DataLakeZoneKey
WHEN MATCHED THEN UPDATE SET
 t.DataLakeZone = s.DataLakeZone
WHEN NOT MATCHED THEN INSERT
(
	 DataLakeZoneKey, DataLakeZone
)
VALUES
(
	 s.DataLakeZoneKey, s.DataLakeZone
);