terraform {
  required_version = ">= 1.5.0"

  # If you use Terraform Cloud for remote state, uncomment and adjust:
  # cloud {
  #   organization = "signalroom"
  #   workspaces {
  #     name = "confluent-kafka-isotope-cc"
  #   }
  # }

  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "2.73.0"
    }
  }
}
