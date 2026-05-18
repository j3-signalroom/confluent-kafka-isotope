variable "confluent_api_key" {
  description = "Confluent Cloud API Key (also referred to as Cloud API ID)."
  type        = string
}

variable "confluent_api_secret" {
  description = "Confluent Cloud API Secret."
  type        = string
  sensitive   = true
}

variable "cloud" {
  description = "Cloud provider hosting the Confluent Cloud resources."
  type        = string
  default     = "AWS"

  validation {
    condition     = contains(["AWS", "GCP", "AZURE"], var.cloud)
    error_message = "`cloud` must be one of AWS, GCP, AZURE."
  }
}

variable "region" {
  description = "Cloud region for the Kafka cluster, Flink compute pool, and artifact."
  type        = string
  default     = "us-east-1"
}

variable "day_count" {
  description = "How many day(s) the Kafka/Flink API keys should be rotated for."
  type        = number
  default     = 30

  validation {
    condition     = var.day_count >= 1
    error_message = "Rolling day count, `day_count`, must be greater than or equal to 1."
  }
}

variable "number_of_api_keys_to_retain" {
  description = "How many API keys to create and retain for rotation. Must be >= 2 to maintain proper rotation."
  type        = number
  default     = 2

  validation {
    condition     = var.number_of_api_keys_to_retain >= 2
    error_message = "`number_of_api_keys_to_retain` must be greater than or equal to 2."
  }
}

variable "artifact_jar_path" {
  description = "Path (relative to terraform/) of the PTF/UDAF shadow JAR built by `./gradlew :ptf:shadowJar`."
  type        = string
  default     = "../ptf/build/libs/isotope-flink-udf.jar"
}
