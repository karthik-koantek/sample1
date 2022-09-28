terraform {
  backend "s3" {
    bucket         = "sigma-connector-s3"  // s3 bucket name
    key            = "tfstate/terraformasc.tfstate"  //folder path in bucket to store state file
    region         = "us-east-2"  // bucket region
    encrypt        = true
    # dynamodb_table = "ktkterraformnew-lock"
   
  }
}
