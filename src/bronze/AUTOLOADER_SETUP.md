# Autoloader Setup: Azure Permissions for File-Notification Mode

The permissions setup, not the Autoloader code itself, is the part most
likely to trip up a first-time setup. This file covers only that.

## Setup sequence

1. **Create an Access Connector for Azure Databricks** in your subscription
   -- this provisions a managed identity Unity Catalog will use.

2. **Grant the Access Connector's managed identity these roles** on the
   ADLS Gen2 storage account:
   - `Storage Blob Data Contributor` -- read/write on file contents.
   - `Storage Queue Data Contributor` -- required for Automatic file events;
     lets Databricks subscribe to file-change notifications.
   - `Storage Account Contributor` -- lets Databricks create the storage
     queue automatically. Skip this only if you're configuring the queue
     yourself manually.
   - `EventGrid Data Contributor` -- lets Databricks configure the Event
     Grid subscription automatically. Skip this only if you're configuring
     the subscription yourself.

3. **Create a Storage Credential** in Unity Catalog referencing the Access
   Connector (Credential Type: Azure Managed Identity).

4. **Create an External Location** pointing at the ADLS Gen2 container,
   using that Storage Credential.

5. **Enable file events on the External Location, mode = Automatic.** This
   is the step that actually turns on notification-based ingestion --
   without it, `cloudFiles.useNotifications` falls back to directory
   listing regardless of what the pipeline code requests.

6. **Create the External Volume** on the team's schema, pointing at the
   registered External Location.

## Common pitfall, worth knowing before you hit it

If the storage account has public network access restricted, Databricks
cannot reach it even with all roles correctly assigned, unless you
explicitly allow Azure trusted services:

```bash
az storage account update \
  --name <storage_account_name> \
  --resource-group <resource_group_name> \
  --bypass AzureServices
```

## What this doesn't cover

Pipeline-side Autoloader configuration (`cloudFiles.format`,
checkpoint handling) is defined in `src/bronze/ingest.py` and is unaffected
by anything in this file -- this document only covers *how Databricks is
allowed to see the storage account*, not how Autoloader behaves once it can.