CREATE PROCEDURE staging.LoadDataQualityValidationResult
AS
BEGIN
    INSERT dbo.DataQualityValidationResult
    (
        StepLogGuid, BatchId, ExpectationSuiteName, ExpectationsVersion, ValidationTime, RunName,
        RunTime, EvaluatedExpectations, SuccessPercent, SuccessfulExpectations, UnsuccessfulExpectations
    )
    SELECT
        src.stepLogGuid, src.batchId, src.expectationSuiteName, src.expectationsVersion, src.validationTime, COALESCE(src.runName,''),
        src.runTime, src.evaluatedExpectations, src.successPercent, src.successfulExpectations, src.unsuccessfulExpectations
    FROM staging.DataQualityValidationResult src
    LEFT JOIN dbo.DataQualityValidationResult tgt ON src.batchId=tgt.BatchId
    WHERE tgt.BatchId IS NULL;
END
GO