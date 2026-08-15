#!/usr/bin/env python3
"""Repair `confluent_flink_statement` credentials that a key rotation invalidated.

Every `confluent_flink_statement` in terraform/setup-confluent-flink.tf carries a
`credentials` block sourced from module.flink_api_key_rotation. Terraform stores
the resolved key/secret in state, and it reads a statement back with the key that
is *in state* — not the one the config would resolve today. The rotation module
retains only `number_of_api_keys_to_retain` keys, so once a statement outlives two
rotations the key recorded against it no longer exists and every plan/apply dies
during refresh with:

    Error: error reading Flink Statement "env-.../lfcp-.../tf-...": 401 Unauthorized

That blocks destroy too, so the environment cannot be torn down either.

The fix is mechanical: any statement whose recorded key is no longer among the
`confluent_api_key` resources in state gets the credentials of a statement whose
key *is* still there. Both belong to the same service account and grant the same
access, so this only restores the ability to read; it changes no infrastructure.

Usage:  tf-repair-flink-credentials.py <path-to-terraform.tfstate>

Exit codes: 0 = state is fine or was repaired, 1 = state unreadable / no donor.
"""

import json
import os
import sys
import tempfile


def main(state_path):
    if not os.path.exists(state_path):
        # No state yet — nothing to repair (first deploy).
        return 0

    with open(state_path) as handle:
        state = json.load(handle)

    resources = state.get("resources", [])

    live_key_ids = {
        instance["attributes"]["id"]
        for resource in resources
        if resource.get("type") == "confluent_api_key"
        for instance in resource.get("instances", [])
        if instance.get("attributes", {}).get("id")
    }

    def statement_instances():
        for resource in resources:
            if resource.get("type") != "confluent_flink_statement":
                continue
            for instance in resource.get("instances", []):
                credentials = instance.get("attributes", {}).get("credentials") or []
                if credentials:
                    yield resource, instance, credentials[0]

    stale = [
        (resource, instance, credentials)
        for resource, instance, credentials in statement_instances()
        if credentials.get("key") not in live_key_ids
    ]

    if not stale:
        return 0

    donor = next(
        (
            credentials
            for _, _, credentials in statement_instances()
            if credentials.get("key") in live_key_ids
        ),
        None,
    )

    if donor is None:
        print(
            "[WARN]  {} Flink statement(s) reference a rotated-out API key and no "
            "statement in state holds a live one — cannot repair automatically. "
            "Expect 401 Unauthorized on refresh.".format(len(stale)),
            file=sys.stderr,
        )
        return 1

    for resource, instance, _ in stale:
        instance["attributes"]["credentials"] = [dict(donor)]
        print(
            "[WARN]  Repaired stale API key on confluent_flink_statement.{}".format(
                resource["name"]
            )
        )

    state["serial"] = state.get("serial", 0) + 1

    directory = os.path.dirname(os.path.abspath(state_path))
    with tempfile.NamedTemporaryFile(
        "w", dir=directory, delete=False, suffix=".tmp"
    ) as handle:
        json.dump(state, handle, indent=2)
        handle.write("\n")
        temporary_path = handle.name
    os.replace(temporary_path, state_path)

    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print(
            "Usage: {} <path-to-terraform.tfstate>".format(
                os.path.basename(sys.argv[0])
            ),
            file=sys.stderr,
        )
        sys.exit(2)
    sys.exit(main(sys.argv[1]))
