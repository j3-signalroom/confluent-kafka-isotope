resource "confluent_environment" "isotope" {
  display_name = local.env_display_name

  stream_governance {
    package = "ESSENTIALS"
  }
}
