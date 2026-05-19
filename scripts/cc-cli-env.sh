#!/usr/bin/env bash
#
# Source this file to load the credentials needed to point the demo CLI
# (and any other Kafka client) at the Confluent Cloud cluster managed by
# terraform/. Exports the values so child processes (gradle JVM) see them
# via $VAR substitution on the command line.
#
# Usage:
#   source scripts/cc-cli-env.sh
#   # or:
#   . scripts/cc-cli-env.sh
#
# Prereqs:
#   - `make cc-flink-reports-up` has succeeded (terraform state exists)
#   - terraform binary on PATH (>= 1.15.x to match terraform/versions.tf)
#   - Both the Kafka and SR API keys are provisioned + rotated by the
#     Terraform module — this script pulls them via `terraform output`.
#     You can still pre-export SR_KEY / SR_SECRET to override the values
#     terraform output gives (useful for personal-key workflows).
#
# After sourcing, fire the four-terminal isotope demo:
#
#   JVM_PROPS=(
#     "-Dkafka.bootstrap=$BOOTSTRAP"
#     "-Dkafka.security.protocol=SASL_SSL"
#     "-Dkafka.sasl.mechanism=PLAIN"
#     "-Dkafka.sasl.jaas.config=$JAAS"
#     "-Dschema.registry.url=$SR_URL"
#     "-Dschema.registry.basic.auth.user.info=$SR_KEY:$SR_SECRET"
#   )
#   ./gradlew :app:run --args="sink iso_final" -q "${JVM_PROPS[@]}"
#   # ... and so on for the three hop / send terminals.
#
# NOTE: requires App.java to read the SASL_SSL + SR-basic-auth system
# properties. As of this script's first commit, that wiring is a known
# follow-up (see README § 3.5 for the demo-CLI status on CCAF).

set -u

# ---------------------------------------------------------------------------
# Locate the repo's terraform/ directory regardless of where this script
# is sourced from. BASH_SOURCE works inside `source`d scripts where $0 is
# the parent shell ("-bash"), not this file.
# ---------------------------------------------------------------------------
__cc_env_script_dir="$(cd "$(dirname "${BASH_SOURCE[0]:-$0}")" && pwd)"
__cc_env_tf_dir="$(cd "${__cc_env_script_dir}/../terraform" && pwd)"

if ! command -v terraform >/dev/null 2>&1; then
    echo "✘ terraform not on PATH — install it (>= 1.15.x) or run scripts/cc-cli-env.sh from a shell where it is."
    return 1 2>/dev/null || exit 1
fi

# ---------------------------------------------------------------------------
# Pull values from terraform state.
#
# `-raw` strips the surrounding quotes terraform output adds by default.
# Sensitive outputs (kafka_api_key, kafka_api_secret) require -raw to
# avoid the "<sensitive>" placeholder.
# ---------------------------------------------------------------------------
BOOTSTRAP="$(terraform -chdir="${__cc_env_tf_dir}" output -raw kafka_bootstrap_servers 2>/dev/null)"
SR_URL="$(terraform -chdir="${__cc_env_tf_dir}" output -raw schema_registry_url 2>/dev/null)"
KAFKA_KEY="$(terraform -chdir="${__cc_env_tf_dir}" output -raw kafka_api_key 2>/dev/null)"
KAFKA_SECRET="$(terraform -chdir="${__cc_env_tf_dir}" output -raw kafka_api_secret 2>/dev/null)"

# Schema Registry credentials — managed by terraform/setup-confluent-kafka.tf's
# `sr_api_key_rotation` module. Honor SR_KEY/SR_SECRET if already exported
# (back-compat for users who minted an SR key in the Cloud Console before
# the Terraform module owned this); otherwise pull from `terraform output`.
SR_KEY="${SR_KEY:-$(terraform -chdir="${__cc_env_tf_dir}" output -raw sr_api_key 2>/dev/null)}"
SR_SECRET="${SR_SECRET:-$(terraform -chdir="${__cc_env_tf_dir}" output -raw sr_api_secret 2>/dev/null)}"

if [ -z "${BOOTSTRAP}" ] || [ -z "${KAFKA_KEY}" ]; then
    echo "✘ terraform outputs are empty — has 'make cc-flink-reports-up' succeeded?"
    echo "  Looked in: ${__cc_env_tf_dir}"
    unset BOOTSTRAP SR_URL KAFKA_KEY KAFKA_SECRET SR_KEY SR_SECRET
    return 1 2>/dev/null || exit 1
fi

if [ -z "${SR_KEY}" ] || [ -z "${SR_SECRET}" ]; then
    echo "⚠ SR_KEY / SR_SECRET still empty after terraform output. The Terraform"
    echo "  module owns the SR API key — if you're on an older state, re-run"
    echo "  'make cc-flink-reports-up' to apply the new sr_api_key_rotation module."
fi

# ---------------------------------------------------------------------------
# Build the SASL JAAS string. Escape rules: the inner double-quotes around
# the username/password need to survive being passed as a -D property
# through gradle to the JVM. Keep them as `\"` here; the shell will expand
# $KAFKA_KEY / $KAFKA_SECRET before the JVM sees the final string.
# ---------------------------------------------------------------------------
JAAS="org.apache.kafka.common.security.plain.PlainLoginModule required username=\"${KAFKA_KEY}\" password=\"${KAFKA_SECRET}\";"

export BOOTSTRAP SR_URL KAFKA_KEY KAFKA_SECRET SR_KEY SR_SECRET JAAS

# ---------------------------------------------------------------------------
# Summary. Masks the secrets so the user can `set -x` debug without leaking.
# ---------------------------------------------------------------------------
__cc_env_mask() {
    local v="$1"
    [ -z "${v}" ] && { echo "<empty>"; return; }
    [ "${#v}" -le 8 ] && { echo "***"; return; }
    echo "${v:0:4}…${v: -4}"
}

echo "✓ CC env loaded from ${__cc_env_tf_dir}"
echo "    BOOTSTRAP    = ${BOOTSTRAP}"
echo "    SR_URL       = ${SR_URL}"
echo "    KAFKA_KEY    = $(__cc_env_mask "${KAFKA_KEY}")"
echo "    KAFKA_SECRET = $(__cc_env_mask "${KAFKA_SECRET}")"
echo "    SR_KEY       = $(__cc_env_mask "${SR_KEY}")"
echo "    SR_SECRET    = $(__cc_env_mask "${SR_SECRET}")"
echo "    JAAS         = (built; \$JAAS exported)"

unset -f __cc_env_mask
unset __cc_env_script_dir __cc_env_tf_dir

set +u
