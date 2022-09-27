terraform {
  backend "s3" {
    bucket         = "ktk-tf-state"  // s3 bucket name
    key            = "tfstate/terraformasc.tfstate"  //folder path in bucket to store state file
    region         = "us-east-2"  // bucket region
    encrypt        = true
    # dynamodb_table = "ktkterraformnew-lock"
   
  }
}
