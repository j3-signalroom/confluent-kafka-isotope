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
# On `create`, always (re)build the PTF shadow JAR so the
# confluent_flink_artifact resource uploads the current code — never a stale
# artifact left over from a prior deploy. Gradle's incremental build makes
# this a no-op when nothing changed.
# ---------------------------------------------------------------------------

if [ "${create_action}" = true ]; then
    print_step "Building PTF shadow JAR via ./gradlew :ptf:shadowJar"
    (cd "${REPO_ROOT}" && ./gradlew :ptf:shadowJar)
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

# ---------------------------------------------------------------------------
# Preflight — repair statement credentials invalidated by key rotation.
#
# Terraform reads a Flink statement back with the API key recorded in STATE, not
# the one the config resolves today. module.flink_api_key_rotation retains only
# `number_of_api_keys_to_retain` keys, so a statement that outlives that many
# rotations points at a deleted key and every refresh dies with
# "401 Unauthorized". This runs on destroy as well as create — teardown
# refreshes too, and a 401 there leaves the environment un-destroyable. See the
# header comment in tf-repair-flink-credentials.py.
# ---------------------------------------------------------------------------
if command -v python3 >/dev/null 2>&1; then
    python3 "${SCRIPT_DIR}/tf-repair-flink-credentials.py" "${TERRAFORM_DIR}/terraform.tfstate" \
        || print_warn "Credential repair reported a problem — a 401 on refresh is likely."
else
    print_warn "python3 not found — skipping the rotated-API-key state repair."
fi

if [ "${create_action}" = true ]; then
    # -----------------------------------------------------------------------
    # Preflight — clear taint left behind by a failed ALTER TABLE ... ADD.
    #
    # CCAF's ALTER TABLE has no per-column IF NOT EXISTS, and the `headers`
    # metadata column the four alter_*_add_headers statements add belongs to
    # the TOPIC's catalog entry, not to the statement resource — destroying or
    # replacing the statement does not remove the column. So any run that tries
    # to (re)create one of those statements against a table that already has
    # the column fails with
    #     Failed to execute ALTER TABLE statement.
    #     Column `headers` already exists in the table.
    # which taints the resource and blocks every downstream statement (views,
    # sinks, INSERTs) on the next apply — forever, since the retry fails the
    # same way. The ALTER is a no-op in that state, so drop the taint and let
    # the apply proceed. If the column genuinely is missing, the problem
    # resurfaces loudly one step later: the isotope_raw view cannot resolve
    # `headers`.
    # -----------------------------------------------------------------------
    while IFS= read -r tf_addr; do
        [ -n "${tf_addr}" ] || continue
        if terraform untaint "${tf_addr}" >/dev/null 2>&1; then
            print_warn "Cleared taint on ${tf_addr} (ALTER TABLE ... ADD is a no-op once the column exists)"
        fi
    done < <(terraform state list 2>/dev/null | grep '_add_headers$' || true)

    # -----------------------------------------------------------------------
    # Preflight — turn illegal in-place statement updates into replacements.
    #
    # The provider can only update a statement's `stopped` /
    # `properties_sensitive`; changing the SQL text, the statement_name or the
    # properties bag is rejected at apply time with
    #     "stopped" or "properties_sensitive" attribute must be updated ...
    # even though `terraform plan` reports a perfectly ordinary in-place update.
    # Those changes are really destroy-and-recreate, so ask for that explicitly.
    # This is what makes editing report SQL in setup-confluent-flink.tf work at
    # all — without it, every SQL edit fails the apply.
    # -----------------------------------------------------------------------
    replace_args=()
    if command -v python3 >/dev/null 2>&1; then
        plan_file="$(mktemp -t isotope-tfplan)"
        if terraform plan -input=false -out="${plan_file}" >/dev/null 2>&1; then
            while IFS= read -r tf_addr; do
                [ -n "${tf_addr}" ] || continue
                print_warn "Forcing replacement of ${tf_addr} (Flink statements cannot be updated in place)"
                replace_args+=("-replace=${tf_addr}")
            done < <(terraform show -json "${plan_file}" \
                        | python3 "${SCRIPT_DIR}/tf-statement-updates.py" || true)
        else
            print_warn "Preflight plan failed — running apply anyway so it reports the reason."
        fi
        rm -f "${plan_file}"
    fi

    # Re-upload of a rebuilt JAR is handled in Terraform: the artifact's
    # display_name embeds the JAR's md5 (a ForceNew attribute), so changed bytes
    # replace the artifact and cascade the PTF drop/recreate. No -replace flag
    # needed — when the code is unchanged the md5 is stable and nothing churns.
    print_step "terraform apply"
    # ${arr[@]+"${arr[@]}"} — expanding an empty array is an error under `set -u`
    # on the bash 3.2 that ships with macOS.
    terraform apply -auto-approve -input=false ${replace_args[@]+"${replace_args[@]}"}

    echo
    print_info "Deploy complete. Non-sensitive outputs:"
    terraform output environment_id
    terraform output kafka_cluster_id
    terraform output kafka_bootstrap_servers
    terraform output schema_registry_url
    terraform output compute_pool_id
    terraform output artifact_id
    echo

    # -----------------------------------------------------------------------
    # Smoke-load scripts/cc-cli-env.sh so the user sees the masked-credential
    # summary before being told to source it themselves. Running it here is
    # a subshell — the exports do NOT propagate to the parent shell that
    # invoked `make cc-flink-reports-up`. The user still has to source it.
    # ----------------------------------------------------------------------
    print_step "Sanity-loading scripts/cc-cli-env.sh (subshell)"
    # shellcheck disable=SC1091
    source "${SCRIPT_DIR}/cc-cli-env.sh" || print_warn "cc-cli-env.sh exited non-zero (see above)"

    terraform graph | dot -Tpng > "../docs/terraform.png"

    echo
    print_info "Next step — drive the 4-stage demo (see root README §3.3):"
    print_info "    scripts/cc-app-run.sh place 'hello'    # kick the chain off"
    print_info "    scripts/cc-app-run.sh enrich"
    print_info "    scripts/cc-app-run.sh fulfill"
    print_info "    scripts/cc-app-run.sh ship             # terminal consumer (emits marker)"

    echo
    print_info "Produce 30 records spaced 5 seconds apart ≈ 2.5 minutes of event-time → spans 3+ windows:"
    print_info "    for i in {1..30}; do scripts/cc-app-run.sh place \"burst-\$i\"; sleep 5; done"
else
    print_step "terraform destroy"
    terraform destroy -auto-approve -input=false
    print_info "Destroy complete."
fi

cd "${REPO_ROOT}"
