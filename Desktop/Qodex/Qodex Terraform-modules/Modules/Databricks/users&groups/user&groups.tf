data "databricks_group" "admins" {
  display_name = "admins"
  depends_on = [azurerm_databricks_workspace.tfadmin-dbwx]
}

resource "databricks_user" "me1" {
  user_name = var.user_name1
}

resource "databricks_user" "me2" {
  user_name = var.user_name2
}

resource "databricks_user" "me3" {
  user_name = var.user_name3
}

resource "databricks_group_member" "a1" {
  group_id  = data.databricks_group.admins.id
  member_id = databricks_user.me3.id
}

resource "databricks_group" "spectators" {
  display_name = var.grp_name_contri
}

resource "databricks_group" "Reader" {
  display_name = var.grp_name_reader
}

resource "databricks_group_member" "a2" {
  group_id  = databricks_group.spectators.id
  member_id = databricks_user.me2.id
}

resource "databricks_group_member" "a3" {
  group_id  = databricks_group.spectators.id
  member_id = databricks_user.me1.id
}

