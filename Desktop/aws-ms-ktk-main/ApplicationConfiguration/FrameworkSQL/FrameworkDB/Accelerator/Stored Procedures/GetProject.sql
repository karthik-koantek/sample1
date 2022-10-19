CREATE PROCEDURE [dbo].[GetProject]
 @ProjectName VARCHAR(255)
AS
BEGIN
  SELECT TOP 100 PERCENT
   p.ProjectKey
  ,p.ProjectName
  ,es.SystemKey
  ,es.SystemName
  ,es.SystemSecretScope
  ,es.SystemOrder
  FROM dbo.Project p
  JOIN dbo.[System] es ON p.ProjectKey=es.ProjectKey
  WHERE p.ProjectName = @ProjectName
  AND es.IsActive = 1
  AND es.IsRestart = 1
  ORDER BY es.SystemOrder;
END

