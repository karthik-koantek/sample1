CREATE FUNCTION dbo.GetWindow(@StepKey BIGINT)
RETURNS @Window TABLE
(
     StepKey BIGINT
    ,StepName VARCHAR(255)
    ,JobKey BIGINT
    ,ParameterName VARCHAR(255)
    ,ParameterValue VARCHAR(255)
)
AS
BEGIN

    DECLARE @StepName VARCHAR(255)
           ,@JobKey BIGINT;

    SELECT @StepName = StepName, @JobKey = JobKey
    FROM dbo.Step
    WHERE StepKey = @StepKey;

    IF @StepName = '../Data Engineering/Raw Zone/Batch SQL'
    BEGIN
        WITH WindowCTE
        AS
        (
            SELECT TOP 1
                 @StepKey AS StepKey
                ,@StepName AS StepName
                ,@JobKey AS JobKey
                ,we.WindowStart AS lowerDateToProcess
                ,CASE WHEN CONVERT(DATE,we.WindowEnd) > CONVERT(DATE,SYSDATETIME()) THEN CONVERT(VARCHAR(20),FORMAT(CONVERT(DATE,SYSDATETIME()),'yyyy/MM/dd')) ELSE we.WindowEnd END AS dateToProcess
            FROM dbo.WindowedExtraction we
            WHERE we.StepKey = @StepKey
            AND we.IsActive = 1
            AND we.ExtractionTimestamp IS NULL
            ORDER BY we.WindowOrder ASC
        )
        INSERT @Window(StepKey, StepName, JobKey, ParameterName, ParameterValue)
        SELECT StepKey, StepName, JobKey, ParameterName, ParameterValue
        FROM WindowCTE
        UNPIVOT
        (
            ParameterValue
            FOR ParameterName IN (lowerDateToProcess, dateToProcess)
        ) AS WindowUnpivot;

        IF (SELECT COUNT(1) FROM @Window) = 0
        BEGIN
            INSERT @Window(StepKey, StepName, JobKey, ParameterName, ParameterValue)
            SELECT @StepKey AS StepKey, @StepName AS StepName, @JobKey AS JobKey, 'lowerDateToProcess' AS ParameterName, MAX(we.WindowEnd) AS ParameterValue
            FROM dbo.WindowedExtraction we
            WHERE we.StepKey = @StepKey
            AND we.IsActive = 1
            AND we.ExtractionTimestamp IS NOT NULL
            GROUP BY we.StepKey
            UNION
            SELECT @StepKey, @StepName, @JobKey, 'dateToProcess', FORMAT(CONVERT(DATE,SYSDATETIME()), 'yyyy/MM/dd');
        END
    END

    IF @StepName = '../DataEngineering/QueryZone/Delta Merge'
    BEGIN
        INSERT @Window(StepKey, StepName, JobKey, ParameterName, ParameterValue)
        SELECT TOP 1 @StepKey, @StepName, @JobKey, 'dateToProcess', WindowEnd
        FROM dbo.WindowedExtraction we
        JOIN dbo.Step s ON we.StepKey=s.StepKey
        WHERE s.JobKey = @JobKey
        AND we.IsActive = 1
        AND we.ExtractionTimestamp IS NOT NULL
        AND we.QueryZoneTimestamp IS NULL
        ORDER BY we.ExtractionTimestamp DESC;
    END

    RETURN
END
GO