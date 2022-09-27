CREATE PROCEDURE [dbo].[GetSystem]
 @SystemName VARCHAR(255)
AS
BEGIN
SELECT TOP 100 PERCENT
   es.SystemKey
  ,es.SystemName
  ,es.ProjectKey
  ,s.StageKey
  ,s.StageName
  ,s.StageOrder
  ,s.NumberOfThreads
  FROM dbo.[System] es
  JOIN dbo.Stage s ON es.SystemKey=s.SystemKey
  WHERE es.SystemName = @SystemName
  AND s.IsActive = 1
  AND s.IsRestart = 1
  ORDER BY s.StageOrder
END