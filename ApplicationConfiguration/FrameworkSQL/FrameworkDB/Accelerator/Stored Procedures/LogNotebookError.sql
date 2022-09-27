CREATE PROCEDURE [dbo].[LogNotebookError]
 @NotebookLogGuid UNIQUEIDENTIFIER
,@Error VARCHAR(MAX)
AS
BEGIN
	 UPDATE dbo.NotebookLog
	 SET
	  LogStatusKey = 3
	 ,EndDateTime = SYSDATETIME()
	 ,Error = @Error
	 WHERE NotebookLogGuid = @NotebookLogGuid;
END
GO
