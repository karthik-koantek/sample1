data "databricks_group" "admins" {
  display_name = "admins"
  depends_on = [var.workspace]
}

resource "databricks_user" "me1" {
  user_name = "eddie.edgeworth@koantek.com"
}

resource "databricks_user" "me5" {
  user_name = "jayanth.chinnam@koantek.com"
}



resource "databricks_user" "me2" {
  user_name = "saptorshe.das@koantek.com"
}

resource "databricks_user" "me3" {
  user_name = "momin.j.umair@koantek.com"
}

resource "databricks_user" "me4" {
  user_name = "prabhat.kumar@koantek.com"
}

resource "databricks_group_member" "a1" {
  group_id  = data.databricks_group.admins.id
  member_id = databricks_user.me4.id
}


resource "databricks_group_member" "a4" {
  group_id  = data.databricks_group.admins.id
  member_id = databricks_user.me3.id
}

resource "databricks_group" "spectators" {
  display_name = "contributor"
}

resource "databricks_group" "Reader" {
  display_name = "Reader"
}


resource "databricks_group_member" "a2" {
  group_id  = databricks_group.spectators.id
  member_id = databricks_user.me2.id
}

resource "databricks_group_member" "a3" {
  group_id  = databricks_group.spectators.id
  member_id = databricks_user.me1.id
}
