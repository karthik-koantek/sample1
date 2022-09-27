EXEC [dbo].[HydrateSpiderMetadataForSQLSource]
 @ClientName='VIP'
,@ServerName='NEXTGEN2\NEXTGEN2'
,@DatabaseName='NGProd'
,@UserName='vec\MidwestVision'
,@PasswordKeyVaultSecretName='VIPPwd'
,@DestinationContainerName='azuredatafactory'
,@SampleRowCount='1000'
,@ExcludeTablesWithZeroRowCount='true'
,@ColumnFilterTerms='acct_id';


--enc_id,acct_id,person_id,practice_id,note_id,document_id,icd_cd,charge_id,template_id