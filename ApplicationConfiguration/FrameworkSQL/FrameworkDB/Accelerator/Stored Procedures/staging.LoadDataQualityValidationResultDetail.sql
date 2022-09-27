CREATE PROCEDURE staging.LoadDataQualityValidationResultDetail
AS
BEGIN
    INSERT dbo.DataQualityValidationResultDetail
    (
         BatchId,Success,ExpectationType,ExceptionMessage,ExceptionTraceback,RaisedException,KwargsColumn,KwargsColumnList,KwargsMaxValue,KwargsMinValue
        ,KwargsMostly,KwargsRegex,KwargsResultFormat,KwargsTypeList,KwargsValueSet,ResultMissingPercent,ResultObservedValue,ResultPartialUnexpectedList
        ,ResultUnexpectedCount,ResultUnexpectedPercent,ResultUnexpectedPercentNonMissing,ResultUnexpectedPercentDouble
    )
    SELECT
         src.batchId,src.success,src.expectationType,src.exceptionMessage,src.exceptionTraceback,src.raisedException,COALESCE(src.kwargsColumn,''),COALESCE(src.kwargsColumnList,''),src.kwargsMaxValue,src.kwargsMinValue
        ,src.kwargsMostly,src.kwargsRegex,src.kwargsResultFormat,src.kwargsTypeList,src.kwargsValueSet,COALESCE(src.resultMissingPercent,0.00),src.resultObservedValue,src.resultPartialUnexpectedList
        ,COALESCE(src.resultUnexpectedCount,0),COALESCE(src.resultUnexpectedPercent,0.00),COALESCE(src.resultUnexpectedPercentNonMissing,0.00),COALESCE(src.resultUnexpectedPercentDouble,0.00)
    FROM staging.DataQualityValidationResultDetail src
    LEFT JOIN dbo.DataQualityValidationResultDetail tgt ON src.batchId=tgt.BatchId
                                                        AND src.expectationType=tgt.ExpectationType
                                                        AND COALESCE(src.kwargsColumn,'')=tgt.KwargsColumn
                                                        AND COALESCE(src.kwargsColumnList,'')=tgt.KwargsColumnList
    WHERE tgt.BatchId IS NULL;
END
GO