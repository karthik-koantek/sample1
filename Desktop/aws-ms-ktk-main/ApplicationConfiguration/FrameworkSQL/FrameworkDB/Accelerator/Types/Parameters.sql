CREATE TYPE [dbo].[Parameters] AS TABLE
(
     ParameterName VARCHAR(255) NOT NULL
    ,ParameterValue VARCHAR(MAX)
);