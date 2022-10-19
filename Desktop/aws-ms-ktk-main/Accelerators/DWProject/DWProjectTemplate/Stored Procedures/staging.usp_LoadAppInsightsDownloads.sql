CREATE PROCEDURE staging.usp_LoadAppInsightsDownloads
AS
BEGIN
-- ====================================================================
-- Author:		Eddie Edgeworth
-- Create Date: 08-22-19
-- Description: Loads the contents of staging.vAppInsightsDownloads into dbo.AppInsightsDownloads.
-- ====================================================================

	DECLARE @Error INT;
	DECLARE @ErrorLine INT;
	DECLARE @Message VARCHAR(500);

	BEGIN TRY
		BEGIN TRAN

		MERGE dbo.AppInsightsDownloads AS tgt
		USING
		(
			SELECT DISTINCT
			 COALESCE(CONVERT(VARCHAR(20),Tool),'') AS Tool
			,COALESCE(CONVERT(VARCHAR(20),contextSessionId),'') AS ContextSessionId
			,CONVERT(DATE,Date) AS [Date]
			,COALESCE(CONVERT(UNIQUEIDENTIFIER,UserID),'00000000-0000-0000-0000-000000000000') AS UserID
			,COALESCE(CONVERT(UNIQUEIDENTIFIER,RoleID),'00000000-0000-0000-0000-000000000000') AS RoleID
			,COALESCE(CONVERT(UNIQUEIDENTIFIER,SegmentID),'00000000-0000-0000-0000-000000000000') AS SegmentID
			,COALESCE(CONVERT(VARCHAR(1000),[URL]),'') AS [Url]
			,COALESCE(CONVERT(CHAR(9),ActionType),'') AS ActionType
			,COALESCE(CONVERT(VARCHAR(1000),City),'') AS City
			,COALESCE(CONVERT(VARCHAR(1000),Country),'') AS Country
			,COALESCE(CONVERT(VARCHAR(100),Province),'') AS Province
			,COALESCE(Count,0) AS [Count]
			FROM staging.vAppInsightsDownloads
		) AS src ON tgt.Tool=src.Tool
				AND tgt.ContextSessionId=src.ContextSessionId
				AND tgt.[Date]=src.[Date]
				AND tgt.[Url]=src.[Url]
		WHEN MATCHED AND tgt.[Count]<>src.[Count]
		THEN UPDATE SET
			tgt.[Count]=src.[Count] --the source data is immutable so expect no updates, except for possibly the count of downloads
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
				,[Url]
				,ActionType
				,[Count]
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
				,src.[Url]
				,src.ActionType
				,src.[Count]
			);

		COMMIT TRAN
	END TRY
	BEGIN CATCH
		SELECT
			 @Error = ERROR_NUMBER()
			,@Message = ERROR_MESSAGE()
			,@ErrorLine = ERROR_LINE();

		RAISERROR('[staging].[usp_LoadAppInsightsDownloads] failed with error %d at line %d: %s',16,1,@Error,@ErrorLine,@Message) WITH NOWAIT;

		IF @@TRANCOUNT > 0
			ROLLBACK TRAN;
	END CATCH
END
GO
