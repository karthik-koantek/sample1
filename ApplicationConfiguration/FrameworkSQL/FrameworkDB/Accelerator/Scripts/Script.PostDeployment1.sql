:r .\LogStatus.sql
:r .\ValidationStatus.sql
:r .\DataLakeZone.sql
:r .\DatabaseCatalog.sql
:r .\ValidationObjectType.sql
GO

--Run this script to grant AD connectivity to the database.  This script fails in the pipeline because the script itself must be run via AD Auth.
--:r .\Scripts\GrantRolePermissionsToADGroups.sql