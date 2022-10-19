CREATE PROCEDURE dbo.RunProject
 @ProjectName VARCHAR(255)
,@JSON VARCHAR(MAX) OUTPUT
AS
BEGIN
    SELECT @JSON =
    (
        SELECT
         p.ProjectKey, p.ProjectName
        ,s.SystemKey, s.SystemName, s.SystemSecretScope, s.SystemOrder
        ,st.StageKey, st.StageName, st.StageOrder, st.NumberOfThreads
        ,j.JobKey, j.JobName, j.JobOrder
        ,stp.StepKey, stp.StepName, stp.StepOrder
        ,pm.ParameterKey, pm.ParameterName, pm.ParameterValue
        FROM dbo.Project p
        JOIN dbo.System s ON p.ProjectKey = s.ProjectKey
        JOIN dbo.Stage st ON s.SystemKey = st.SystemKey
        JOIN dbo.Job j ON st.StageKey = j.StageKey
        JOIN dbo.Step stp ON j.JobKey = stp.JobKey
        JOIN dbo.Parameter pm ON stp.StepKey = pm.StepKey
        WHERE p.ProjectName = @ProjectName
        AND s.IsActive = 1 AND st.IsActive = 1 AND j.IsActive = 1 AND stp.IsActive = 1
        AND s.IsRestart = 1 AND st.IsRestart = 1 AND j.IsRestart = 1 AND stp.IsRestart = 1
        FOR JSON AUTO, WITHOUT_ARRAY_WRAPPER
    );
END
GO

