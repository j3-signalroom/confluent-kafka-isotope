terraform {
  required_version = ">= 1.13.0"

  # If you use Terraform Cloud for remote state, uncomment and adjust:
  # cloud {
  #   organization = "signalroom"
  #   workspaces {
  #     name = "confluent-kafka-isotope"
  #   }
  # }

  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "2.75.0"
    }
  }
}
