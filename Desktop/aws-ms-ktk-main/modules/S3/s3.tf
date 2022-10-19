/*-------------------------------------------------- *
 S3 Buckets *
 The resource block will create all the buckets in the variable array
 *-------------------------------------------------*/
resource "aws_s3_bucket" "s3ascend" {
    bucket = "ktk-transient-s3"
    acl = "private"
    # region = var.aws_region
    force_destroy = true
}

resource "aws_s3_bucket" "s3ascend1" {
    bucket = "ktk-bronze-s3"
    acl = "private"
    # region = var.aws_region
    force_destroy = true
}

resource "aws_s3_bucket" "s3ascend2" {
    bucket = "ktk-silvergold-s3"
    acl = "private"
    # region = var.aws_region
    force_destroy = true
}

resource "aws_s3_bucket" "s3ascend3" {
    bucket = "ktk-sandbox-s3"
    acl = "private"
    # region = var.aws_region
    force_destroy = true
}
######################################################################################folders transient
resource "aws_s3_object" "object-t1" {
  bucket = "ktk-transient-s3"
  key    = "staging"
  depends_on   = [aws_s3_bucket.s3ascend]
}
resource "aws_s3_object" "object-t2" {
  bucket = "ktk-transient-s3"
  key    = "polybase"
  depends_on   = [aws_s3_bucket.s3ascend]
}
resource "aws_s3_object" "object-t3" {
  bucket = "ktk-transient-s3"
  key    = "fivetran"
  depends_on   = [aws_s3_bucket.s3ascend]
}
resource "aws_s3_object" "object-t4" {
  bucket = "ktk-transient-s3"
  key    = "azuredatafactory"
  depends_on   = [aws_s3_bucket.s3ascend]
}
##################################################bronze
			
resource "aws_s3_object" "object-b1" {
  bucket = "ktk-bronze-s3"
  key    = "raw"
  depends_on   = [aws_s3_bucket.s3ascend1]
}
########################################silver
resource "aws_s3_object" "object-s1" {
  bucket = "ktk-silvergold-s3"
  key    = "silvergeneral"
  depends_on   = [aws_s3_bucket.s3ascend2]
}
resource "aws_s3_object" "object-s2" {
  bucket = "ktk-silvergold-s3"
  key    = "silverprotected"
  depends_on   = [aws_s3_bucket.s3ascend2]
}
resource "aws_s3_object" "object-s3" {
  bucket = "ktk-silvergold-s3"
  key    = "goldgeneral"
  depends_on   = [aws_s3_bucket.s3ascend2]
}
resource "aws_s3_object" "object-s4" {
  bucket = "ktk-silvergold-s3"
  key    = "goldprotected"
  depends_on   = [aws_s3_bucket.s3ascend2]
}
resource "aws_s3_object" "object-s5" {
  bucket = "ktk-silvergold-s3"
  key    = "archive"
  depends_on   = [aws_s3_bucket.s3ascend2]
}
#####################################################sandbox
resource "aws_s3_object" "object-sb1" {
  bucket = "ktk-sandbox-s3"
  key    = "defaultsandbox"
  depends_on   = [aws_s3_bucket.s3ascend3]
}
