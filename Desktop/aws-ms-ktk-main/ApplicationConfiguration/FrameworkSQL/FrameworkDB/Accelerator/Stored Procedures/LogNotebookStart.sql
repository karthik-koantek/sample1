CREATE PROCEDURE [dbo].[LogNotebookStart]
 @NotebookLogGuid UNIQUEIDENTIFIER
,@StepLogGuid UNIQUEIDENTIFIER
,@StepKey BIGINT
,@Parameters VARCHAR(MAX)
,@Context VARCHAR(MAX)
AS
BEGIN
	IF @StepLogGuid = '00000000-0000-0000-0000-000000000000'
		SET @StepLogGuid = NULL;

	IF @StepKey = -1
		SET @StepKey = NULL;

	INSERT dbo.NotebookLog
	(
		 NotebookLogGuid
		,StepLogGuid
		,StepKey
		,LogStatusKey
		,Parameters
		,Context
	)
	VALUES
	(
		 @NotebookLogGuid
		,@StepLogGuid
		,@StepKey
		,1
		,@Parameters
		,@Context
	);
END
GO