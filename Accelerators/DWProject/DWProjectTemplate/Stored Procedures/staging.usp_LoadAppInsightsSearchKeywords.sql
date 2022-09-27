CREATE PROCEDURE staging.usp_LoadAppInsightsSearchKeywords
AS
BEGIN
-- ====================================================================
-- Author:		Eddie Edgeworth
-- Create Date: 08-22-19
-- Description: Loads the contents of staging.vAppInsightsSearchKeywords into dbo.AppInsightsSearchKeywords.
-- ====================================================================

	DECLARE @Error INT;
	DECLARE @ErrorLine INT;
	DECLARE @Message VARCHAR(500);

	BEGIN TRY
		BEGIN TRAN

		MERGE dbo.AppInsightsSearchKeywords AS tgt
		USING
		(
			SELECT DISTINCT
			 COALESCE(CONVERT(VARCHAR(20),Tool),'') AS Tool
			,COALESCE(CONVERT(VARCHAR(20),contextSessionId),'') AS ContextSessionId
			,CONVERT(DATE,Date) AS [Date]
			,COALESCE(CONVERT(UNIQUEIDENTIFIER,UserID),'00000000-0000-0000-0000-000000000000') AS UserID
			,COALESCE(CONVERT(UNIQUEIDENTIFIER,RoleID),'00000000-0000-0000-0000-000000000000') AS RoleID
			,COALESCE(CONVERT(UNIQUEIDENTIFIER,SegmentID),'00000000-0000-0000-0000-000000000000') AS SegmentID
			,COALESCE(CONVERT(VARCHAR(255),SearchKeyword),'') AS SearchKeyword
			,COALESCE(CONVERT(VARCHAR(1000),City),'') AS City
			,COALESCE(CONVERT(VARCHAR(1000),Country),'') AS Country
			,COALESCE(CONVERT(VARCHAR(100),Province),'') AS Province
			,COALESCE(Count,0) AS [Count]
			FROM staging.vAppInsightsSearchKeywords
		) AS src ON tgt.Tool=src.Tool
				AND tgt.ContextSessionId=src.ContextSessionId
				AND tgt.[Date]=src.[Date]
				AND tgt.City=src.City
				AND tgt.Province=src.Province
				AND tgt.RoleID=src.RoleID
				AND tgt.SegmentID=src.SegmentID
				AND tgt.SearchKeyword=src.SearchKeyword
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
				,SearchKeyword
				,City
				,Country
				,Province
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
				,src.SearchKeyword
				,src.City
				,src.Country
				,src.Province
				,src.[Count]
			);

		COMMIT TRAN
	END TRY
	BEGIN CATCH
		SELECT
			 @Error = ERROR_NUMBER()
			,@Message = ERROR_MESSAGE()
			,@ErrorLine = ERROR_LINE();

		RAISERROR('[staging].[usp_LoadAppInsightsSearchKeywords] failed with error %d at line %d: %s',16,1,@Error,@ErrorLine,@Message) WITH NOWAIT;

		IF @@TRANCOUNT > 0
			ROLLBACK TRAN;
	END CATCH
END
GO