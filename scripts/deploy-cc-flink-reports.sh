#!/bin/bash
#
# Deploy (or destroy) the Confluent Cloud for Apache Flink (CCAF) side of
# the isotope project. Mirrors the role of scripts/deploy-flink-reports.sh
# for CP Flink, but here the orchestration is Terraform-driven —
# terraform/ contains the resources.
#
# *** Script syntax ***
#   ./deploy-cc-flink-reports.sh <create|destroy>
#       --confluent-api-key=<CONFLUENT_CLOUD_API_KEY>
#       --confluent-api-secret=<CONFLUENT_CLOUD_API_SECRET>
#       [--day-count=<DAY_COUNT>]
#
# The Confluent Cloud API key must have permissions to manage environments,
# Kafka clusters, Flink compute pools, service accounts, role bindings, and
# Flink artifacts/statements in the target organization.
#

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NO_COLOR='\033[0m'

print_info()  { echo -e "${GREEN}[INFO]${NO_COLOR}  $1"; }
print_warn()  { echo -e "${YELLOW}[WARN]${NO_COLOR}  $1"; }
print_error() { echo -e "${RED}[ERROR]${NO_COLOR} $1"; }
print_step()  { echo -e "${BLUE}[STEP]${NO_COLOR}  $1"; }

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
TERRAFORM_DIR="${REPO_ROOT}/terraform"
JAR_PATH="${REPO_ROOT}/ptf/build/libs/isotope-flink-udf.jar"

argument_list="--confluent-api-key=<KEY> --confluent-api-secret=<SECRET> [--day-count=<DAYS>]"

usage_and_exit() {
    echo
    print_error "Usage: $(basename "$0") <create|destroy> ${argument_list}"
    echo
    exit 85
}

# ---------------------------------------------------------------------------
# Parse the create|destroy command
# ---------------------------------------------------------------------------

if [ $# -lt 1 ]; then
    print_error "(Error 001) Missing command. Specify create or destroy."
    usage_and_exit
fi

case "$1" in
    create)  create_action=true ;;
    destroy) create_action=false ;;
    *)
        print_error "(Error 002) Unknown command: $1"
        usage_and_exit
        ;;
esac

# ---------------------------------------------------------------------------
# Parse named arguments
# ---------------------------------------------------------------------------

day_count=30
confluent_api_key=""
confluent_api_secret=""

shift
for arg in "$@"; do
    case $arg in
        --confluent-api-key=*)
            confluent_api_key="${arg#--confluent-api-key=}"
            ;;
        --confluent-api-secret=*)
            confluent_api_secret="${arg#--confluent-api-secret=}"
            ;;
        --day-count=*)
            day_count="${arg#--day-count=}"
            ;;
        *)
            print_error "(Error 003) Invalid argument: $arg"
            usage_and_exit
            ;;
    esac
done

if [ -z "${confluent_api_key}" ]; then
    print_error "(Error 004) --confluent-api-key=<KEY> is required."
    usage_and_exit
fi

if [ -z "${confluent_api_secret}" ]; then
    print_error "(Error 005) --confluent-api-secret=<SECRET> is required."
    usage_and_exit
fi

# ---------------------------------------------------------------------------
# Pre-flight — Terraform installed?
# ---------------------------------------------------------------------------

if ! command -v terraform >/dev/null 2>&1; then
    print_error "terraform not found on PATH. Install from https://developer.hashicorp.com/terraform/install."
    exit 86
fi

print_info "Terraform directory: ${TERRAFORM_DIR}"
print_info "Repo root:           ${REPO_ROOT}"

# ---------------------------------------------------------------------------
# On `create`, build the PTF/UDAF shadow JAR if missing so the
# confluent_flink_artifact resource has something to upload.
# ---------------------------------------------------------------------------

if [ "${create_action}" = true ]; then
    if [ ! -f "${JAR_PATH}" ]; then
        print_step "Shadow JAR not found at ${JAR_PATH} — building via ./gradlew :ptf:shadowJar"
        (cd "${REPO_ROOT}" && ./gradlew :ptf:shadowJar)
    else
        print_info "Shadow JAR present at ${JAR_PATH}"
    fi
fi

# ---------------------------------------------------------------------------
# Export Terraform variables and run the apply/destroy
# ---------------------------------------------------------------------------

export TF_VAR_confluent_api_key="${confluent_api_key}"
export TF_VAR_confluent_api_secret="${confluent_api_secret}"
export TF_VAR_day_count="${day_count}"

cd "${TERRAFORM_DIR}"

print_step "terraform init"
terraform init -input=false

if [ "${create_action}" = true ]; then
    print_step "terraform apply"
    terraform apply -auto-approve -input=false

    echo
    print_info "Deploy complete. Non-sensitive outputs:"
    terraform output environment_id
    terraform output kafka_cluster_id
    terraform output kafka_bootstrap_servers
    terraform output schema_registry_url
    terraform output compute_pool_id
    terraform output artifact_id
    echo

    # ----------------------------------------------------------------------
    # Smoke-load scripts/cc-cli-env.sh so the user sees the masked-credential
    # summary before being told to source it themselves. Running it here is
    # a subshell — the exports do NOT propagate to the parent shell that
    # invoked `make cc-flink-reports-up`. The user still has to source it.
    # ----------------------------------------------------------------------
    print_step "Sanity-loading scripts/cc-cli-env.sh (subshell)"
    # shellcheck disable=SC1091
    source "${SCRIPT_DIR}/cc-cli-env.sh" || print_warn "cc-cli-env.sh exited non-zero (see above)"

    echo
    print_info "Next step — source the helper in your own shell to export the env vars:"
    print_info "    source scripts/cc-cli-env.sh"
    print_info "Then drive the 3-stage demo (see README § 3.5)."

    print_info "Creating the Terraform visualization..."
    terraform graph | dot -Tpng > "$TERRAFORM_DIR/terraform.png"
    print_info "Terraform visualization created at: $TERRAFORM_DIR/terraform.png"
else
    print_step "terraform destroy"
    terraform destroy -auto-approve -input=false
    print_info "Destroy complete."
fi

cd "${REPO_ROOT}"
