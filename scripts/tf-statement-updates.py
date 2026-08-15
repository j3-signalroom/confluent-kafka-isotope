#!/usr/bin/env python3
"""List `confluent_flink_statement` resources a plan wants to update in place.

The Confluent provider can only update a statement's `stopped` or
`properties_sensitive` attribute. Any other change — the SQL text, the
`statement_name`, the properties bag — is rejected at apply time with

    Error: error updating Flink Statement "...": "stopped" or
    "properties_sensitive" attribute must be updated for Flink Statement

even though `terraform plan` cheerfully reports it as an in-place update. Such a
change is really a destroy-and-recreate, so the deploy script feeds these
addresses back to `terraform apply` as `-replace=` arguments.

Updates that only touch `stopped` / `properties_sensitive` are legal and are
left alone.

Usage:  terraform show -json <planfile> | tf-statement-updates.py
"""

import json
import sys

UPDATABLE_ATTRIBUTES = {"stopped", "properties_sensitive"}


def changed_attributes(change):
    before = change.get("before") or {}
    after = change.get("after") or {}
    unknown = change.get("after_unknown") or {}
    names = set(before) | set(after) | set(unknown)
    return {
        name
        for name in names
        if before.get(name) != after.get(name) or unknown.get(name)
    }


def main():
    plan = json.load(sys.stdin)

    for resource_change in plan.get("resource_changes", []):
        if resource_change.get("type") != "confluent_flink_statement":
            continue

        change = resource_change.get("change", {})
        if change.get("actions") != ["update"]:
            continue

        if changed_attributes(change) - UPDATABLE_ATTRIBUTES:
            print(resource_change["address"])

    return 0


if __name__ == "__main__":
    sys.exit(main())
