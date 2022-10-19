CREATE PROCEDURE [dbo].[TruncateHydration]
 @IncludeLogs BIT = 0
,@Confirm CHAR(7)
AS
BEGIN
    DECLARE @ErrorMessage NVARCHAR(MAX);
    DECLARE @Error INT;
    DECLARE @ReturnCode INT;

	IF @Confirm = 'CONFIRM'
	BEGIN
		IF @IncludeLogs = 1
			BEGIN
			DELETE FROM dbo.ValidationLog;
			DELETE FROM dbo.NotebookLog;
			DELETE FROM dbo.StepLog;
			DELETE FROM dbo.JobLog;
			DELETE FROM dbo.StageLog;
			DELETE FROM dbo.SystemLog;
			DELETE FROM dbo.ProjectLog;
		END

		DELETE dbo.WindowedExtraction;
		DELETE dbo.[Validation];
		DELETE dbo.Parameter;
		DELETE dbo.Step;
		DELETE dbo.Job;
		DELETE dbo.Stage;
		DELETE dbo.[System];
		DELETE dbo.Project;
	END
	ELSE
	BEGIN
		SET @ErrorMessage = 'This will truncate ALL Hydration Data.  Supply the parameter @Confirm = ''CONFIRM'' to confirm.' + CHAR(13) + CHAR(10) + ' ';
		RAISERROR(@ErrorMessage,16,1) WITH NOWAIT;
		SET @Error = @@ERROR;
	END
END
GO
