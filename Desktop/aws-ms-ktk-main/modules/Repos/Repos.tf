 resource "databricks_git_credential" "git" {
   git_username          = "sohini.avirneni@koantek.com"
   git_provider          = "gitLab"
   personal_access_token = "glpat-NobLKFGrwRnk9EFQehG5"
 }

 resource "databricks_repo" "databricks-repo" {
   url = "https://gitlab.com/Koantek/aws-ms-ktk.git"
   // path = "/Repos/nintex-feature/"
   git_provider = "gitLab"
   branch = "main"
 }
