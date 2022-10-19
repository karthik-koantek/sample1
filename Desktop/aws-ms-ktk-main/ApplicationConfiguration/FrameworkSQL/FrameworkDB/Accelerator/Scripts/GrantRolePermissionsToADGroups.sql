/*

Script to grant windows AD Groups permissions within the Database.

This script must be run under Windows MFA Authentication by a user who has admin permissions on the database.

Run this against the metadata database and the Data Warehouse Database.
*/

DECLARE @OwnerGroup VARCHAR(100);
DECLARE @ContributorGroup VARCHAR(100);
DECLARE @ReaderGroup VARCHAR(100);
DECLARE @ResourceGroupName VARCHAR(100);
SET @ResourceGroupName = REPLACE(@@SERVERNAME, '-sql', '-rg');

SET @OwnerGroup = @ResourceGroupName + ' Sanctioned Zone Owners';
SET @ContributorGroup = @ResourceGroupName + ' Sanctioned Zone Contributors';
SET @ReaderGroup = @ResourceGroupName + ' Sanctioned Zone Readers';

IF NOT EXISTS (SELECT 1 FROM SYS.database_principals WHERE name = @OwnerGroup)
    EXEC('CREATE USER [' + @OwnerGroup + '] FROM EXTERNAL PROVIDER;');

IF NOT EXISTS (SELECT 1 FROM SYS.database_principals WHERE name = @ContributorGroup)
    EXEC('CREATE USER [' + @ContributorGroup + '] FROM EXTERNAL PROVIDER;');

IF NOT EXISTS (SELECT 1 FROM SYS.database_principals WHERE name = @ReaderGroup)
    EXEC('CREATE USER [' + @ReaderGroup + '] FROM EXTERNAL PROVIDER;');

EXEC('ALTER ROLE db_owner ADD MEMBER [' + @OwnerGroup + '];');

EXEC('ALTER ROLE db_datareader ADD MEMBER [' + @ContributorGroup + '];');
EXEC('ALTER ROLE db_datawriter ADD MEMBER [' + @ContributorGroup + '];');
EXEC('GRANT EXECUTE TO [' + @ContributorGroup + '];');

EXEC('ALTER ROLE db_datareader ADD MEMBER [' + @ReaderGroup + '];');
