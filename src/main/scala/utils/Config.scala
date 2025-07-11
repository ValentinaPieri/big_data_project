package utils

object Config {

  // The local directory containing this repository
  val projectDir :String = "/C:/Users/valen/Documents/uni/big_data/flight_analysis_project"
  // The name of the shared bucket on AWS S3 to read datasets
  // val s3sharedBucketName :String = "unibo-bd2425-egallinucci"
  // The name of your bucket on AWS S3
  val s3bucketName :String = "vpieri"
  // The path to the credentials file for AWS (if you follow instructions, this should not be updated)
  val credentialsPath :String = "/aws_credentials.txt"

}
