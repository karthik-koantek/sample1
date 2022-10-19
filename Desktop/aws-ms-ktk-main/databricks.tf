data "aws_availability_zones" "available" {}

########mws network####
###################subnets#######
resource "aws_subnet" "pvt1ip" {
  vpc_id     = var.vpc_id
  cidr_block = var.cidr_1
}


resource "aws_subnet" "pvt2ip" {
  vpc_id     = var.vpc_id
  cidr_block = var.cidr_2
}

############security grp############
resource "aws_security_group" "sg" {
  name        = "${local.prefix}-sg"
  description = "Allow TLS inbound traffic"
  vpc_id      = var.vpc_id

  ingress {
    from_port        = 22
    to_port          = 22
    protocol         = "tcp"
    cidr_blocks      = ["10.159.0.0/16"]

  }

  egress {
    from_port        = 0
    to_port          = 0
    protocol         = "-1"
    cidr_blocks      = ["0.0.0.0/0"]
  }
}


resource "databricks_mws_networks" "thisnet" {
  provider           = databricks.mws
  account_id         = var.databricks_account_id
  network_name       = "${local.prefix}-network"
  security_group_ids = [aws_security_group.sg.id] // Security group ID
  subnet_ids         = [aws_subnet.pvt1ip.id,aws_subnet.pvt2ip.id] // subnet ID
  vpc_id             = var.vpc_id// VPC ID
  depends_on       = [aws_subnet.pvt1ip, aws_subnet.pvt2ip, aws_security_group.sg]
}


############mws s3#######
resource "aws_s3_bucket" "root_storage_bucket" {
  bucket = "${local.prefix}-dbwsbucket"
  acl    = "private"
  versioning {
    enabled = false
  }
  force_destroy = true
  tags = merge(var.tags, {
    Name = "${local.prefix}-dbwsbucket"
  })
}

resource "aws_s3_bucket_server_side_encryption_configuration" "root_storage_bucket" {
  bucket = aws_s3_bucket.root_storage_bucket.bucket

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "root_storage_bucket" {
  bucket                  = aws_s3_bucket.root_storage_bucket.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
  depends_on              = [aws_s3_bucket.root_storage_bucket]
}

data "databricks_aws_bucket_policy" "this" {
  bucket = aws_s3_bucket.root_storage_bucket.bucket
}

resource "aws_s3_bucket_policy" "root_bucket_policy" {
  bucket     = aws_s3_bucket.root_storage_bucket.id
  policy     = data.databricks_aws_bucket_policy.this.json
  depends_on = [aws_s3_bucket_public_access_block.root_storage_bucket]
}


resource "databricks_mws_storage_configurations" "thisstrg" {
  provider                   = databricks.mws
  account_id                 = var.databricks_account_id
  bucket_name                = aws_s3_bucket.root_storage_bucket.bucket
  storage_configuration_name = "${local.prefix}-storage"
  depends_on                 = [aws_s3_bucket.root_storage_bucket]
}

########mws credetnial######
data "databricks_aws_assume_role_policy" "thisiam" {
  external_id = var.databricks_account_id
}

resource "aws_iam_role" "cross_account_role" {
  name               = "${local.prefix}-crossaccountas"
  assume_role_policy = data.databricks_aws_assume_role_policy.thisiam.json
  tags               = var.tags
}

data "databricks_aws_crossaccount_policy" "thiscr" {
}

resource "aws_iam_role_policy" "thispl" {
  name   = "${local.prefix}-policy"
  role   = aws_iam_role.cross_account_role.id
  policy = data.databricks_aws_crossaccount_policy.thiscr.json
}


resource "databricks_mws_credentials" "thiscred" {
  provider         = databricks.mws
  account_id       = var.databricks_account_id
  role_arn         = aws_iam_role.cross_account_role.arn
  credentials_name = "${local.prefix}-creds"
  depends_on       = [aws_iam_role_policy.thispl]
}
resource "time_sleep" "wait" {
  depends_on = [
    aws_iam_role.cross_account_role
  ]
  create_duration = "120s"
}



##########mws worspaece#####
resource "databricks_mws_workspaces" "this" {
  provider       = databricks.mws
  account_id     = var.databricks_account_id
  aws_region     = var.region
  workspace_name = "${local.prefix}-dbws" // Workspace Name

  credentials_id           = databricks_mws_credentials.thiscred.credentials_id // credential id passed by using API
  storage_configuration_id = databricks_mws_storage_configurations.thisstrg.storage_configuration_id
  network_id               = databricks_mws_networks.thisnet.network_id

  token {
    comment = "Terraform"
  }
}

output "databricks_host" {
  value = databricks_mws_workspaces.this.workspace_url
}

output "databricks_token" {
  value     = databricks_mws_workspaces.this.token[0].token_value
  sensitive = true
}



# resource "databricks_token" "pat1" {
#   provider = databricks.created_workspace
#   comment  = "Terraform Provisioning"
#   lifetime_seconds = 864000
#   depends_on = [databricks_mws_workspaces.this]
# }
