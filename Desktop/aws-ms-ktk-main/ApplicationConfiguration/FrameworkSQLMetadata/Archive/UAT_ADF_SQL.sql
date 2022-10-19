--EXEC dbo.HydrateADFSQLMetadataQueries @ProjectName='UAT_ADF_SQL',@SystemName='UAT_ADF_SQL',@StageName='UAT_ADF_SQL_Collect',@ServerName='dbvdc.database.windows.net',@DatabaseName='dbvdc',@UserName='dbvdc',@PasswordKeyVaultSecretName='ExternalDatabasePwd',@DestinationContainerName='azuredatafactory',@DestinationBasePath='adfsqlcollect/';

--TABLES
EXEC [dbo].[HydrateADFSQL]   @ProjectName = 'UAT_ADF_SQL'  ,@SystemName = 'UAT_ADF_SQL'  ,@SystemOrder = 10  ,@SystemIsActive = 1  ,@StageName = 'UAT_ADF_SQL_Daily_Tables'  ,@StageIsActive = 1  ,@StageOrder = 10  ,@JobName = 'UAT_ADF_SQL_Daily_table_dbo_BuildVersion'  ,@JobOrder = 10  ,@JobIsActive = 1  ,@ServerName = 'dbvdc.database.windows.net'  ,@DatabaseName = 'dbvdc'  ,@UserName = 'dbvdc'  ,@PasswordKeyVaultSecretName = 'ExternalDatabasePwd'  ,@SchemaName = 'dbo'  ,@TableName = 'BuildVersion'  ,@DestinationContainerName = 'azuredatafactory'  ,@DestinationFilePath = 'adfsqlcollect/table/dbo/BuildVersion'  ,@ADFPipelineName = 'On Premises Database Table to Staging';
EXEC [dbo].[HydrateADFSQL]   @ProjectName = 'UAT_ADF_SQL'  ,@SystemName = 'UAT_ADF_SQL'  ,@SystemOrder = 10  ,@SystemIsActive = 1  ,@StageName = 'UAT_ADF_SQL_Daily_Tables'  ,@StageIsActive = 1  ,@StageOrder = 10  ,@JobName = 'UAT_ADF_SQL_Daily_table_dbo_ErrorLog'  ,@JobOrder = 10  ,@JobIsActive = 0  ,@ServerName = 'dbvdc.database.windows.net'  ,@DatabaseName = 'dbvdc'  ,@UserName = 'dbvdc'  ,@PasswordKeyVaultSecretName = 'ExternalDatabasePwd'  ,@SchemaName = 'dbo'  ,@TableName = 'ErrorLog'  ,@DestinationContainerName = 'azuredatafactory'  ,@DestinationFilePath = 'adfsqlcollect/table/dbo/ErrorLog'  ,@ADFPipelineName = 'On Premises Database Table to Staging';
EXEC [dbo].[HydrateADFSQL]   @ProjectName = 'UAT_ADF_SQL'  ,@SystemName = 'UAT_ADF_SQL'  ,@SystemOrder = 10  ,@SystemIsActive = 1  ,@StageName = 'UAT_ADF_SQL_Daily_Tables'  ,@StageIsActive = 1  ,@StageOrder = 10  ,@JobName = 'UAT_ADF_SQL_Daily_table_SalesLT_Address'  ,@JobOrder = 10  ,@JobIsActive = 0  ,@ServerName = 'dbvdc.database.windows.net'  ,@DatabaseName = 'dbvdc'  ,@UserName = 'dbvdc'  ,@PasswordKeyVaultSecretName = 'ExternalDatabasePwd'  ,@SchemaName = 'SalesLT'  ,@TableName = 'Address'  ,@DestinationContainerName = 'azuredatafactory'  ,@DestinationFilePath = 'adfsqlcollect/table/SalesLT/Address'  ,@ADFPipelineName = 'On Premises Database Table to Staging';
EXEC [dbo].[HydrateADFSQL]   @ProjectName = 'UAT_ADF_SQL'  ,@SystemName = 'UAT_ADF_SQL'  ,@SystemOrder = 10  ,@SystemIsActive = 1  ,@StageName = 'UAT_ADF_SQL_Daily_Tables'  ,@StageIsActive = 1  ,@StageOrder = 10  ,@JobName = 'UAT_ADF_SQL_Daily_table_SalesLT_Customer'  ,@JobOrder = 10  ,@JobIsActive = 0  ,@ServerName = 'dbvdc.database.windows.net'  ,@DatabaseName = 'dbvdc'  ,@UserName = 'dbvdc'  ,@PasswordKeyVaultSecretName = 'ExternalDatabasePwd'  ,@SchemaName = 'SalesLT'  ,@TableName = 'Customer'  ,@DestinationContainerName = 'azuredatafactory'  ,@DestinationFilePath = 'adfsqlcollect/table/SalesLT/Customer'  ,@ADFPipelineName = 'On Premises Database Table to Staging';
EXEC [dbo].[HydrateADFSQL]   @ProjectName = 'UAT_ADF_SQL'  ,@SystemName = 'UAT_ADF_SQL'  ,@SystemOrder = 10  ,@SystemIsActive = 1  ,@StageName = 'UAT_ADF_SQL_Daily_Tables'  ,@StageIsActive = 1  ,@StageOrder = 10  ,@JobName = 'UAT_ADF_SQL_Daily_table_SalesLT_CustomerAddress'  ,@JobOrder = 10  ,@JobIsActive = 0  ,@ServerName = 'dbvdc.database.windows.net'  ,@DatabaseName = 'dbvdc'  ,@UserName = 'dbvdc'  ,@PasswordKeyVaultSecretName = 'ExternalDatabasePwd'  ,@SchemaName = 'SalesLT'  ,@TableName = 'CustomerAddress'  ,@DestinationContainerName = 'azuredatafactory'  ,@DestinationFilePath = 'adfsqlcollect/table/SalesLT/CustomerAddress'  ,@ADFPipelineName = 'On Premises Database Table to Staging';
EXEC [dbo].[HydrateADFSQL]   @ProjectName = 'UAT_ADF_SQL'  ,@SystemName = 'UAT_ADF_SQL'  ,@SystemOrder = 10  ,@SystemIsActive = 1  ,@StageName = 'UAT_ADF_SQL_Daily_Tables'  ,@StageIsActive = 1  ,@StageOrder = 10  ,@JobName = 'UAT_ADF_SQL_Daily_table_SalesLT_Product'  ,@JobOrder = 10  ,@JobIsActive = 0  ,@ServerName = 'dbvdc.database.windows.net'  ,@DatabaseName = 'dbvdc'  ,@UserName = 'dbvdc'  ,@PasswordKeyVaultSecretName = 'ExternalDatabasePwd'  ,@SchemaName = 'SalesLT'  ,@TableName = 'Product'  ,@DestinationContainerName = 'azuredatafactory'  ,@DestinationFilePath = 'adfsqlcollect/table/SalesLT/Product'  ,@ADFPipelineName = 'On Premises Database Table to Staging';
EXEC [dbo].[HydrateADFSQL]   @ProjectName = 'UAT_ADF_SQL'  ,@SystemName = 'UAT_ADF_SQL'  ,@SystemOrder = 10  ,@SystemIsActive = 1  ,@StageName = 'UAT_ADF_SQL_Daily_Tables'  ,@StageIsActive = 1  ,@StageOrder = 10  ,@JobName = 'UAT_ADF_SQL_Daily_table_SalesLT_ProductCategory'  ,@JobOrder = 10  ,@JobIsActive = 0  ,@ServerName = 'dbvdc.database.windows.net'  ,@DatabaseName = 'dbvdc'  ,@UserName = 'dbvdc'  ,@PasswordKeyVaultSecretName = 'ExternalDatabasePwd'  ,@SchemaName = 'SalesLT'  ,@TableName = 'ProductCategory'  ,@DestinationContainerName = 'azuredatafactory'  ,@DestinationFilePath = 'adfsqlcollect/table/SalesLT/ProductCategory'  ,@ADFPipelineName = 'On Premises Database Table to Staging';
EXEC [dbo].[HydrateADFSQL]   @ProjectName = 'UAT_ADF_SQL'  ,@SystemName = 'UAT_ADF_SQL'  ,@SystemOrder = 10  ,@SystemIsActive = 1  ,@StageName = 'UAT_ADF_SQL_Daily_Tables'  ,@StageIsActive = 1  ,@StageOrder = 10  ,@JobName = 'UAT_ADF_SQL_Daily_table_SalesLT_ProductDescription'  ,@JobOrder = 10  ,@JobIsActive = 0  ,@ServerName = 'dbvdc.database.windows.net'  ,@DatabaseName = 'dbvdc'  ,@UserName = 'dbvdc'  ,@PasswordKeyVaultSecretName = 'ExternalDatabasePwd'  ,@SchemaName = 'SalesLT'  ,@TableName = 'ProductDescription'  ,@DestinationContainerName = 'azuredatafactory'  ,@DestinationFilePath = 'adfsqlcollect/table/SalesLT/ProductDescription'  ,@ADFPipelineName = 'On Premises Database Table to Staging';
EXEC [dbo].[HydrateADFSQL]   @ProjectName = 'UAT_ADF_SQL'  ,@SystemName = 'UAT_ADF_SQL'  ,@SystemOrder = 10  ,@SystemIsActive = 1  ,@StageName = 'UAT_ADF_SQL_Daily_Tables'  ,@StageIsActive = 1  ,@StageOrder = 10  ,@JobName = 'UAT_ADF_SQL_Daily_table_SalesLT_ProductModel'  ,@JobOrder = 10  ,@JobIsActive = 0  ,@ServerName = 'dbvdc.database.windows.net'  ,@DatabaseName = 'dbvdc'  ,@UserName = 'dbvdc'  ,@PasswordKeyVaultSecretName = 'ExternalDatabasePwd'  ,@SchemaName = 'SalesLT'  ,@TableName = 'ProductModel'  ,@DestinationContainerName = 'azuredatafactory'  ,@DestinationFilePath = 'adfsqlcollect/table/SalesLT/ProductModel'  ,@ADFPipelineName = 'On Premises Database Table to Staging';
EXEC [dbo].[HydrateADFSQL]   @ProjectName = 'UAT_ADF_SQL'  ,@SystemName = 'UAT_ADF_SQL'  ,@SystemOrder = 10  ,@SystemIsActive = 1  ,@StageName = 'UAT_ADF_SQL_Daily_Tables'  ,@StageIsActive = 1  ,@StageOrder = 10  ,@JobName = 'UAT_ADF_SQL_Daily_table_SalesLT_ProductModelProductDescription'  ,@JobOrder = 10  ,@JobIsActive = 0  ,@ServerName = 'dbvdc.database.windows.net'  ,@DatabaseName = 'dbvdc'  ,@UserName = 'dbvdc'  ,@PasswordKeyVaultSecretName = 'ExternalDatabasePwd'  ,@SchemaName = 'SalesLT'  ,@TableName = 'ProductModelProductDescription'  ,@DestinationContainerName = 'azuredatafactory'  ,@DestinationFilePath = 'adfsqlcollect/table/SalesLT/ProductModelProductDescription'  ,@ADFPipelineName = 'On Premises Database Table to Staging';
EXEC [dbo].[HydrateADFSQL]   @ProjectName = 'UAT_ADF_SQL'  ,@SystemName = 'UAT_ADF_SQL'  ,@SystemOrder = 10  ,@SystemIsActive = 1  ,@StageName = 'UAT_ADF_SQL_Daily_Tables'  ,@StageIsActive = 1  ,@StageOrder = 10  ,@JobName = 'UAT_ADF_SQL_Daily_table_SalesLT_SalesOrderDetail'  ,@JobOrder = 10  ,@JobIsActive = 0  ,@ServerName = 'dbvdc.database.windows.net'  ,@DatabaseName = 'dbvdc'  ,@UserName = 'dbvdc'  ,@PasswordKeyVaultSecretName = 'ExternalDatabasePwd'  ,@SchemaName = 'SalesLT'  ,@TableName = 'SalesOrderDetail'  ,@DestinationContainerName = 'azuredatafactory'  ,@DestinationFilePath = 'adfsqlcollect/table/SalesLT/SalesOrderDetail'  ,@ADFPipelineName = 'On Premises Database Table to Staging';
EXEC [dbo].[HydrateADFSQL]   @ProjectName = 'UAT_ADF_SQL'  ,@SystemName = 'UAT_ADF_SQL'  ,@SystemOrder = 10  ,@SystemIsActive = 1  ,@StageName = 'UAT_ADF_SQL_Daily_Tables'  ,@StageIsActive = 1  ,@StageOrder = 10  ,@JobName = 'UAT_ADF_SQL_Daily_table_SalesLT_SalesOrderHeader'  ,@JobOrder = 10  ,@JobIsActive = 0  ,@ServerName = 'dbvdc.database.windows.net'  ,@DatabaseName = 'dbvdc'  ,@UserName = 'dbvdc'  ,@PasswordKeyVaultSecretName = 'ExternalDatabasePwd'  ,@SchemaName = 'SalesLT'  ,@TableName = 'SalesOrderHeader'  ,@DestinationContainerName = 'azuredatafactory'  ,@DestinationFilePath = 'adfsqlcollect/table/SalesLT/SalesOrderHeader'  ,@ADFPipelineName = 'On Premises Database Table to Staging';

--SAMPLES/QUERIES
EXEC [dbo].[HydrateADFSQL]   @ProjectName = 'UAT_ADF_SQL'  ,@SystemName = 'UAT_ADF_SQL'  ,@SystemOrder = 10  ,@SystemIsActive = 1  ,@StageName = 'UAT_ADF_SQL_Daily_Queries'  ,@StageIsActive = 1  ,@StageOrder = 20  ,@JobName = 'UAT_ADF_SQL_Daily_query_dbo_BuildVersion'  ,@JobOrder = 10  ,@JobIsActive = 1  ,@ServerName = 'dbvdc.database.windows.net'  ,@DatabaseName = 'dbvdc'  ,@UserName = 'dbvdc'  ,@PasswordKeyVaultSecretName = 'ExternalDatabasePwd'  ,@SchemaName = 'dbo'  ,@TableName = 'BuildVersion'  ,@PushdownQuery = 'SELECT TOP 1000 * FROM [dbo].[BuildVersion]'  ,@DestinationContainerName = 'azuredatafactory'  ,@DestinationFilePath = 'adfsqlcollect/query/dbo/BuildVersion'  ,@ADFPipelineName = 'On Premises Database Query to Staging';
EXEC [dbo].[HydrateADFSQL]   @ProjectName = 'UAT_ADF_SQL'  ,@SystemName = 'UAT_ADF_SQL'  ,@SystemOrder = 10  ,@SystemIsActive = 1  ,@StageName = 'UAT_ADF_SQL_Daily_Queries'  ,@StageIsActive = 1  ,@StageOrder = 20  ,@JobName = 'UAT_ADF_SQL_Daily_query_dbo_ErrorLog'  ,@JobOrder = 10  ,@JobIsActive = 0  ,@ServerName = 'dbvdc.database.windows.net'  ,@DatabaseName = 'dbvdc'  ,@UserName = 'dbvdc'  ,@PasswordKeyVaultSecretName = 'ExternalDatabasePwd'  ,@SchemaName = 'dbo'  ,@TableName = 'ErrorLog'  ,@PushdownQuery = 'SELECT TOP 1000 * FROM [dbo].[ErrorLog]'  ,@DestinationContainerName = 'azuredatafactory'  ,@DestinationFilePath = 'adfsqlcollect/query/dbo/ErrorLog'  ,@ADFPipelineName = 'On Premises Database Query to Staging';
EXEC [dbo].[HydrateADFSQL]   @ProjectName = 'UAT_ADF_SQL'  ,@SystemName = 'UAT_ADF_SQL'  ,@SystemOrder = 10  ,@SystemIsActive = 1  ,@StageName = 'UAT_ADF_SQL_Daily_Queries'  ,@StageIsActive = 1  ,@StageOrder = 20  ,@JobName = 'UAT_ADF_SQL_Daily_query_SalesLT_Address'  ,@JobOrder = 10  ,@JobIsActive = 0  ,@ServerName = 'dbvdc.database.windows.net'  ,@DatabaseName = 'dbvdc'  ,@UserName = 'dbvdc'  ,@PasswordKeyVaultSecretName = 'ExternalDatabasePwd'  ,@SchemaName = 'SalesLT'  ,@TableName = 'Address'  ,@PushdownQuery = 'SELECT TOP 1000 * FROM [SalesLT].[Address]'  ,@DestinationContainerName = 'azuredatafactory'  ,@DestinationFilePath = 'adfsqlcollect/query/SalesLT/Address'  ,@ADFPipelineName = 'On Premises Database Query to Staging';
EXEC [dbo].[HydrateADFSQL]   @ProjectName = 'UAT_ADF_SQL'  ,@SystemName = 'UAT_ADF_SQL'  ,@SystemOrder = 10  ,@SystemIsActive = 1  ,@StageName = 'UAT_ADF_SQL_Daily_Queries'  ,@StageIsActive = 1  ,@StageOrder = 20  ,@JobName = 'UAT_ADF_SQL_Daily_query_SalesLT_Customer'  ,@JobOrder = 10  ,@JobIsActive = 0  ,@ServerName = 'dbvdc.database.windows.net'  ,@DatabaseName = 'dbvdc'  ,@UserName = 'dbvdc'  ,@PasswordKeyVaultSecretName = 'ExternalDatabasePwd'  ,@SchemaName = 'SalesLT'  ,@TableName = 'Customer'  ,@PushdownQuery = 'SELECT TOP 1000 * FROM [SalesLT].[Customer]'  ,@DestinationContainerName = 'azuredatafactory'  ,@DestinationFilePath = 'adfsqlcollect/query/SalesLT/Customer'  ,@ADFPipelineName = 'On Premises Database Query to Staging';
EXEC [dbo].[HydrateADFSQL]   @ProjectName = 'UAT_ADF_SQL'  ,@SystemName = 'UAT_ADF_SQL'  ,@SystemOrder = 10  ,@SystemIsActive = 1  ,@StageName = 'UAT_ADF_SQL_Daily_Queries'  ,@StageIsActive = 1  ,@StageOrder = 20  ,@JobName = 'UAT_ADF_SQL_Daily_query_SalesLT_CustomerAddress'  ,@JobOrder = 10  ,@JobIsActive = 0  ,@ServerName = 'dbvdc.database.windows.net'  ,@DatabaseName = 'dbvdc'  ,@UserName = 'dbvdc'  ,@PasswordKeyVaultSecretName = 'ExternalDatabasePwd'  ,@SchemaName = 'SalesLT'  ,@TableName = 'CustomerAddress'  ,@PushdownQuery = 'SELECT TOP 1000 * FROM [SalesLT].[CustomerAddress]'  ,@DestinationContainerName = 'azuredatafactory'  ,@DestinationFilePath = 'adfsqlcollect/query/SalesLT/CustomerAddress'  ,@ADFPipelineName = 'On Premises Database Query to Staging';
EXEC [dbo].[HydrateADFSQL]   @ProjectName = 'UAT_ADF_SQL'  ,@SystemName = 'UAT_ADF_SQL'  ,@SystemOrder = 10  ,@SystemIsActive = 1  ,@StageName = 'UAT_ADF_SQL_Daily_Queries'  ,@StageIsActive = 1  ,@StageOrder = 20  ,@JobName = 'UAT_ADF_SQL_Daily_query_SalesLT_Product'  ,@JobOrder = 10  ,@JobIsActive = 0  ,@ServerName = 'dbvdc.database.windows.net'  ,@DatabaseName = 'dbvdc'  ,@UserName = 'dbvdc'  ,@PasswordKeyVaultSecretName = 'ExternalDatabasePwd'  ,@SchemaName = 'SalesLT'  ,@TableName = 'Product'  ,@PushdownQuery = 'SELECT TOP 1000 * FROM [SalesLT].[Product]'  ,@DestinationContainerName = 'azuredatafactory'  ,@DestinationFilePath = 'adfsqlcollect/query/SalesLT/Product'  ,@ADFPipelineName = 'On Premises Database Query to Staging';
EXEC [dbo].[HydrateADFSQL]   @ProjectName = 'UAT_ADF_SQL'  ,@SystemName = 'UAT_ADF_SQL'  ,@SystemOrder = 10  ,@SystemIsActive = 1  ,@StageName = 'UAT_ADF_SQL_Daily_Queries'  ,@StageIsActive = 1  ,@StageOrder = 20  ,@JobName = 'UAT_ADF_SQL_Daily_query_SalesLT_ProductCategory'  ,@JobOrder = 10  ,@JobIsActive = 0  ,@ServerName = 'dbvdc.database.windows.net'  ,@DatabaseName = 'dbvdc'  ,@UserName = 'dbvdc'  ,@PasswordKeyVaultSecretName = 'ExternalDatabasePwd'  ,@SchemaName = 'SalesLT'  ,@TableName = 'ProductCategory'  ,@PushdownQuery = 'SELECT TOP 1000 * FROM [SalesLT].[ProductCategory]'  ,@DestinationContainerName = 'azuredatafactory'  ,@DestinationFilePath = 'adfsqlcollect/query/SalesLT/ProductCategory'  ,@ADFPipelineName = 'On Premises Database Query to Staging';
EXEC [dbo].[HydrateADFSQL]   @ProjectName = 'UAT_ADF_SQL'  ,@SystemName = 'UAT_ADF_SQL'  ,@SystemOrder = 10  ,@SystemIsActive = 1  ,@StageName = 'UAT_ADF_SQL_Daily_Queries'  ,@StageIsActive = 1  ,@StageOrder = 20  ,@JobName = 'UAT_ADF_SQL_Daily_query_SalesLT_ProductDescription'  ,@JobOrder = 10  ,@JobIsActive = 0  ,@ServerName = 'dbvdc.database.windows.net'  ,@DatabaseName = 'dbvdc'  ,@UserName = 'dbvdc'  ,@PasswordKeyVaultSecretName = 'ExternalDatabasePwd'  ,@SchemaName = 'SalesLT'  ,@TableName = 'ProductDescription'  ,@PushdownQuery = 'SELECT TOP 1000 * FROM [SalesLT].[ProductDescription]'  ,@DestinationContainerName = 'azuredatafactory'  ,@DestinationFilePath = 'adfsqlcollect/query/SalesLT/ProductDescription'  ,@ADFPipelineName = 'On Premises Database Query to Staging';
EXEC [dbo].[HydrateADFSQL]   @ProjectName = 'UAT_ADF_SQL'  ,@SystemName = 'UAT_ADF_SQL'  ,@SystemOrder = 10  ,@SystemIsActive = 1  ,@StageName = 'UAT_ADF_SQL_Daily_Queries'  ,@StageIsActive = 1  ,@StageOrder = 20  ,@JobName = 'UAT_ADF_SQL_Daily_query_SalesLT_ProductModel'  ,@JobOrder = 10  ,@JobIsActive = 0  ,@ServerName = 'dbvdc.database.windows.net'  ,@DatabaseName = 'dbvdc'  ,@UserName = 'dbvdc'  ,@PasswordKeyVaultSecretName = 'ExternalDatabasePwd'  ,@SchemaName = 'SalesLT'  ,@TableName = 'ProductModel'  ,@PushdownQuery = 'SELECT TOP 1000 * FROM [SalesLT].[ProductModel]'  ,@DestinationContainerName = 'azuredatafactory'  ,@DestinationFilePath = 'adfsqlcollect/query/SalesLT/ProductModel'  ,@ADFPipelineName = 'On Premises Database Query to Staging';
EXEC [dbo].[HydrateADFSQL]   @ProjectName = 'UAT_ADF_SQL'  ,@SystemName = 'UAT_ADF_SQL'  ,@SystemOrder = 10  ,@SystemIsActive = 1  ,@StageName = 'UAT_ADF_SQL_Daily_Queries'  ,@StageIsActive = 1  ,@StageOrder = 20  ,@JobName = 'UAT_ADF_SQL_Daily_query_SalesLT_ProductModelProductDescription'  ,@JobOrder = 10  ,@JobIsActive = 0  ,@ServerName = 'dbvdc.database.windows.net'  ,@DatabaseName = 'dbvdc'  ,@UserName = 'dbvdc'  ,@PasswordKeyVaultSecretName = 'ExternalDatabasePwd'  ,@SchemaName = 'SalesLT'  ,@TableName = 'ProductModelProductDescription'  ,@PushdownQuery = 'SELECT TOP 1000 * FROM [SalesLT].[ProductModelProductDescription]'  ,@DestinationContainerName = 'azuredatafactory'  ,@DestinationFilePath = 'adfsqlcollect/query/SalesLT/ProductModelProductDescription'  ,@ADFPipelineName = 'On Premises Database Query to Staging';
EXEC [dbo].[HydrateADFSQL]   @ProjectName = 'UAT_ADF_SQL'  ,@SystemName = 'UAT_ADF_SQL'  ,@SystemOrder = 10  ,@SystemIsActive = 1  ,@StageName = 'UAT_ADF_SQL_Daily_Queries'  ,@StageIsActive = 1  ,@StageOrder = 20  ,@JobName = 'UAT_ADF_SQL_Daily_query_SalesLT_SalesOrderDetail'  ,@JobOrder = 10  ,@JobIsActive = 0  ,@ServerName = 'dbvdc.database.windows.net'  ,@DatabaseName = 'dbvdc'  ,@UserName = 'dbvdc'  ,@PasswordKeyVaultSecretName = 'ExternalDatabasePwd'  ,@SchemaName = 'SalesLT'  ,@TableName = 'SalesOrderDetail'  ,@PushdownQuery = 'SELECT TOP 1000 * FROM [SalesLT].[SalesOrderDetail]'  ,@DestinationContainerName = 'azuredatafactory'  ,@DestinationFilePath = 'adfsqlcollect/query/SalesLT/SalesOrderDetail'  ,@ADFPipelineName = 'On Premises Database Query to Staging';
EXEC [dbo].[HydrateADFSQL]   @ProjectName = 'UAT_ADF_SQL'  ,@SystemName = 'UAT_ADF_SQL'  ,@SystemOrder = 10  ,@SystemIsActive = 1  ,@StageName = 'UAT_ADF_SQL_Daily_Queries'  ,@StageIsActive = 1  ,@StageOrder = 20  ,@JobName = 'UAT_ADF_SQL_Daily_query_SalesLT_SalesOrderHeader'  ,@JobOrder = 10  ,@JobIsActive = 0  ,@ServerName = 'dbvdc.database.windows.net'  ,@DatabaseName = 'dbvdc'  ,@UserName = 'dbvdc'  ,@PasswordKeyVaultSecretName = 'ExternalDatabasePwd'  ,@SchemaName = 'SalesLT'  ,@TableName = 'SalesOrderHeader'  ,@PushdownQuery = 'SELECT TOP 1000 * FROM [SalesLT].[SalesOrderHeader]'  ,@DestinationContainerName = 'azuredatafactory'  ,@DestinationFilePath = 'adfsqlcollect/query/SalesLT/SalesOrderHeader'  ,@ADFPipelineName = 'On Premises Database Query to Staging';

--PROCEDURES