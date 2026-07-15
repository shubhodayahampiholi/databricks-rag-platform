# Prerequisites

Everything below must be in place before this bundle can be deployed for a
new application team. All of it maps directly to the Prerequisites section
of [`docs/DESIGN.md`](docs/DESIGN.md).

## 1. Azure-side setup

- A dedicated Azure resource group for the team.
- An ADLS Gen2 storage account and container within that resource group.
- A Databricks Access Connector, granted `Storage Blob Data Contributor`
  and `Storage Queue Data Contributor` on the storage account — this is
  what allows Unity Catalog to read the team's ADLS Gen2 container.

## 2. Unity Catalog registration

- The ADLS Gen2 container registered as a Unity Catalog **External
  Location**, using a storage credential built on the Access Connector
  above.
- File events enabled on the external location, in **Automatic** mode —
  this is the prerequisite for Autoloader's file-notification ingestion
  mode used throughout bronze.

## 3. Catalog and schema

- A dedicated Unity Catalog **catalog** and **schema** for the team.
- An **External Volume** on that schema, pointing at the team's ADLS Gen2
  container — this is the governed, zero-copy path the team's source files
  are accessed through.
- A separate **Managed Volume** on the same schema, reserved for pipeline
  checkpoints — deliberately never nested inside the external volume above.

## 4. Access and permissions

- `CREATE STORAGE CREDENTIAL` and `CREATE EXTERNAL LOCATION` privileges
  granted to whoever is provisioning the team (typically a workspace or
  metastore admin, per Unity Catalog's default privilege model).
- A `usage_policy_id` assigned to the team, for cost attribution — see the
  Cost Governance section of the design doc.

## 5. Tooling

- Databricks CLI installed, with an authenticated profile for the target
  workspace.
- Access to run `databricks bundle validate` and `databricks bundle deploy`
  against at least a `dev` target before promotion to `prod`.

## Note on environment

This platform assumes a workspace with real external cloud storage access
(a paid or trial Databricks workspace with an Azure subscription attached).
Databricks Free Edition does not support custom external storage locations
and cannot host this architecture as designed.