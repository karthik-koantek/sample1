EXEC [dbo].[HydrateADFSQL] @ProjectName='Spider_VIP_1_ADF',@SystemName='Spider_VIP_NEXTGEN2_NEXTGEN2_1_ADF',@SystemOrder=10,@SystemIsActive=1,@StageName='Spider_VIP_NEXTGEN2_NEXTGEN2_Sample_1_ADF',@StageIsActive=1,@StageOrder=10,@JobName='Spider_VIP_NEXTGEN2_NEXTGEN2_Sample_1_ADF_sample_dbo_accounts',@JobOrder=10,@JobIsActive=1,@ServerName='NEXTGEN2\NEXTGEN2',@DatabaseName='NGProd',@UserName='vec\MidwestVision',@PasswordKeyVaultSecretName='VIPPwd',@SchemaName='dbo',@TableName='accounts',@PushdownQuery='SELECT TOP 1000 * FROM [dbo].[accounts]',@DestinationContainerName='azuredatafactory',@DestinationFilePath='spider/VIP/NEXTGEN2_NEXTGEN2/sample/dbo/accounts',@ADFPipelineName='On Premises Database Query to Staging'
EXEC [dbo].[HydrateADFSQL] @ProjectName='Spider_VIP_1_ADF',@SystemName='Spider_VIP_NEXTGEN2_NEXTGEN2_1_ADF',@SystemOrder=10,@SystemIsActive=1,@StageName='Spider_VIP_NEXTGEN2_NEXTGEN2_Sample_1_ADF',@StageIsActive=1,@StageOrder=10,@JobName='Spider_VIP_NEXTGEN2_NEXTGEN2_Sample_1_ADF_sample_dbo_accounts_bak_20081121',@JobOrder=10,@JobIsActive=1,@ServerName='NEXTGEN2\NEXTGEN2',@DatabaseName='NGProd',@UserName='vec\MidwestVision',@PasswordKeyVaultSecretName='VIPPwd',@SchemaName='dbo',@TableName='accounts_bak_20081121',@PushdownQuery='SELECT TOP 1000 * FROM [dbo].[accounts_bak_20081121]',@DestinationContainerName='azuredatafactory',@DestinationFilePath='spider/VIP/NEXTGEN2_NEXTGEN2/sample/dbo/accounts_bak_20081121',@ADFPipelineName='On Premises Database Query to Staging'
EXEC [dbo].[HydrateADFSQL] @ProjectName='Spider_VIP_1_ADF',@SystemName='Spider_VIP_NEXTGEN2_NEXTGEN2_1_ADF',@SystemOrder=10,@SystemIsActive=1,@StageName='Spider_VIP_NEXTGEN2_NEXTGEN2_Sample_1_ADF',@StageIsActive=1,@StageOrder=10,@JobName='Spider_VIP_NEXTGEN2_NEXTGEN2_Sample_1_ADF_sample_dbo_statement_history_header',@JobOrder=10,@JobIsActive=1,@ServerName='NEXTGEN2\NEXTGEN2',@DatabaseName='NGProd',@UserName='vec\MidwestVision',@PasswordKeyVaultSecretName='VIPPwd',@SchemaName='dbo',@TableName='statement_history_header',@PushdownQuery='SELECT TOP 1000 * FROM [dbo].[statement_history_header]',@DestinationContainerName='azuredatafactory',@DestinationFilePath='spider/VIP/NEXTGEN2_NEXTGEN2/sample/dbo/statement_history_header',@ADFPipelineName='On Premises Database Query to Staging'
EXEC [dbo].[HydrateADFSQL] @ProjectName='Spider_VIP_1_ADF',@SystemName='Spider_VIP_NEXTGEN2_NEXTGEN2_1_ADF',@SystemOrder=10,@SystemIsActive=1,@StageName='Spider_VIP_NEXTGEN2_NEXTGEN2_Sample_1_ADF',@StageIsActive=1,@StageOrder=10,@JobName='Spider_VIP_NEXTGEN2_NEXTGEN2_Sample_1_ADF_sample_dbo_statements',@JobOrder=10,@JobIsActive=1,@ServerName='NEXTGEN2\NEXTGEN2',@DatabaseName='NGProd',@UserName='vec\MidwestVision',@PasswordKeyVaultSecretName='VIPPwd',@SchemaName='dbo',@TableName='statements',@PushdownQuery='SELECT TOP 1000 * FROM [dbo].[statements]',@DestinationContainerName='azuredatafactory',@DestinationFilePath='spider/VIP/NEXTGEN2_NEXTGEN2/sample/dbo/statements',@ADFPipelineName='On Premises Database Query to Staging'