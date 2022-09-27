CREATE PROCEDURE [dbo].[LogValidationLog]
 @ValidationLogGuid UNIQUEIDENTIFIER
,@StepLogGuid UNIQUEIDENTIFIER
,@ValidationKey BIGINT
,@ValidationStatus VARCHAR(200)
,@Parameters VARCHAR(MAX)
,@Error VARCHAR(MAX)
AS
BEGIN
	IF @StepLogGuid = '00000000-0000-0000-0000-000000000000'
		SET @StepLogGuid = NULL;

    DECLARE @ValidationStatusKey TINYINT = NULL;
    SELECT @ValidationStatusKey = ValidationStatusKey FROM dbo.ValidationStatus WHERE ValidationStatus = @ValidationStatus;

	INSERT dbo.ValidationLog
	(
		 ValidationLogGuid
        ,StepLogGuid
        ,ValidationKey
        ,ValidationStatusKey
		,Parameters
        ,Error
	)
	VALUES
	(
		 @ValidationLogGuid
		,@StepLogGuid
		,@ValidationKey
		,@ValidationStatusKey
		,@Parameters
		,@Error
	);
END
GO