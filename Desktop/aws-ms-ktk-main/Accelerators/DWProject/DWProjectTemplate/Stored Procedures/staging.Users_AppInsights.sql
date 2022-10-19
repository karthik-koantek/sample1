CREATE PROCEDURE staging.usp_LoadUsers_AppInsights
AS
BEGIN
-- ====================================================================
-- Author:		Eddie Edgeworth
-- Create Date: 08-22-19
-- Description: Loads the contents of staging.vUsers_AppInsights into dbo.Users_AppInsights.
-- ====================================================================

	DECLARE @Error INT;
	DECLARE @ErrorLine INT;
	DECLARE @Message VARCHAR(500);

	BEGIN TRY
		BEGIN TRAN

		MERGE dbo.Users_AppInsights AS tgt
		USING
		(
			SELECT DISTINCT
			 COALESCE(CONVERT(VARCHAR(20),Tool),'') AS Tool
			,COALESCE(CONVERT(VARCHAR(20),ContextSessionId),'') AS ContextSessionId
			,CONVERT(DATE,Date) AS [Date]
			,COALESCE(CONVERT(UNIQUEIDENTIFIER,UserID),'00000000-0000-0000-0000-000000000000') AS UserID
			,COALESCE(CONVERT(UNIQUEIDENTIFIER,RoleID),'00000000-0000-0000-0000-000000000000') AS RoleID
			,COALESCE(CONVERT(UNIQUEIDENTIFIER,SegmentID),'00000000-0000-0000-0000-000000000000') AS SegmentID
			,COALESCE(CONVERT(VARCHAR(100),City),'') AS City
			,COALESCE(CONVERT(VARCHAR(100),Country),'') AS Country
			,COALESCE(CONVERT(VARCHAR(100),Province),'') AS Province
			FROM staging.vUsers_AppInsights
		) AS src ON tgt.Tool=src.Tool
				AND tgt.ContextSessionId=src.ContextSessionId
				AND tgt.[Date]=src.[Date]
				AND tgt.UserID=src.UserID
				AND tgt.RoleID=src.RoleID
				AND tgt.SegmentID=src.SegmentID
				AND tgt.City=src.City
				AND tgt.Country=src.Country
				AND tgt.Province=src.Province
		WHEN NOT MATCHED BY TARGET THEN
			INSERT
			(
				 Tool
				,ContextSessionId
				,[Date]
				,UserID
				,RoleID
				,SegmentID
				,City
				,Country
				,Province
			)
			VALUES
			(
				 src.Tool
				,src.ContextSessionId
				,src.[Date]
				,src.UserID
				,src.RoleID
				,src.SegmentID
				,src.City
				,src.Country
				,src.Province
			);

		COMMIT TRAN
	END TRY
	BEGIN CATCH
		SELECT
			 @Error = ERROR_NUMBER()
			,@Message = ERROR_MESSAGE()
			,@ErrorLine = ERROR_LINE();

		RAISERROR('[staging].[usp_LoadUsers_AppInsights] failed with error %d at line %d: %s',16,1,@Error,@ErrorLine,@Message) WITH NOWAIT;

		IF @@TRANCOUNT > 0
			ROLLBACK TRAN;
	END CATCH
END
GO