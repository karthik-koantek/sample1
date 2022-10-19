CREATE FUNCTION dbo.fn_GetParameter(@StepKey BIGINT)
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
    INSERT @Window(StepKey, StepName, JobKey, ParameterName, ParameterValue)
    SELECT
    s.StepKey
    ,s.StepName
    ,s.JobKey
    ,p.ParameterName
    ,p.ParameterValue
    FROM dbo.Step s
    JOIN dbo.Parameter p ON s.StepKey=p.StepKey
    WHERE s.StepKey = @StepKey
    UNION
    SELECT StepKey, StepName, JobKey, ParameterName, ParameterValue
    FROM dbo.GetWindow(@StepKey)

    RETURN
END
GO
