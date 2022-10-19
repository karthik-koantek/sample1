CREATE PROCEDURE [dbo].[LogNotebookEnd]
 @NotebookLogGuid UNIQUEIDENTIFIER
,@RowsAffected BIGINT = NULL
AS
BEGIN
	UPDATE dbo.NotebookLog
	SET
	 LogStatusKey = 2
	,EndDateTime = SYSDATETIME()
	,RowsAffected = @RowsAffected
	WHERE NotebookLogGuid = @NotebookLogGuid;
END
GO
