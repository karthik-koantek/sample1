
 resource "databricks_git_credential" "git" {
   git_username          = var.git_username
   git_provider          = var.git_provider
   personal_access_token = var.personal_access_token
 }

 resource "databricks_repo" "databricks-repo" {
   url = var.url
   git_provider = var.git_provider
   branch = var.branch
 }

