# Scalable IAM User Storage & Migration

## Overview

SeaweedFS IAM has transitioned from a monolithic configuration file to a scalable, file-based storage backend for user identities. This document details the architecture, storage layout, and the automatic migration process that enables this scalability.


## Architecture: Monolithic vs. Scalable

### Legacy: Monolithic `identity.json`
Previously, all user identities, credentials, and policies were stored in a single `/etc/iam/identity.json` file.
*   **Limitation 1 (Concurrency):** Any update (creating a user, rotating a key) required reading, locking, modifying, and writing the entire file.
*   **Limitation 2 (Scalability):** As the number of users grew, the size of `identity.json` increased, causing higher memory usage and slower parsing times.
*   **Limitation 3 (Risk):** Corruption of this single file could affect all users.

### New: Per-User File Storage
The new architecture stores each user as an individual JSON file within the Filer.
*   **Scalability:** The Filer efficiently handles millions of files. Operations on one user do not affect others.
*   **Performance:** Reading or updating a specific user only requires I/O for that user's small JSON file.
*   **Concurrency:** Multiple parallel operations can occur simultaneously on different users without contention.

## Storage Layout

The IAM data is organized in the Filer under `/etc/iam/`:

```
/
├── etc/
│   └── iam/
│       ├── identity.json     # Global configuration (Account Groups & settings)
│       ├── users/            # Directory containing individual user files
│       │   ├── alice.json    # Complete identity definition for user 'alice'
│       │   ├── bob.json      # Complete identity definition for user 'bob'
│       │   └── ...
│       └── accounts/         # Directory containing individual account files
│           ├── acc123.json   # Definition for account 'acc123'
│           └── ...
│       └── service_accounts/ # Directory containing individual service account files
│           ├── sa-123.json   # Definition for service account 'sa-123'
│           └── ...
```

### User File Format (`<username>.json`)
Each file contains the full protocol buffer definition for a single identity in JSON format:

```json
{
  "name": "alice",
  "credentials": [
    {
      "accessKey": "AKIA...",
      "secretKey": "SECRET...",
      "status": "Active"
    }
  ],
  "actions": [
    "Read",
    "Write",
    "List"
  ]
}
```

### Account File Format (`<account_id>.json`)
Each file contains the definition for a single account:

```json
{
  "id": "acc123",
  "displayName": "Test Account",
  "emailAddress": "test@example.com"
}
```

### Service Account File Format (`<service_account_id>.json`)
Each file contains the definition for a single service account:

```json
{
  "id": "sa-123",
  "parentUser": "alice",
  "credential": {
    "accessKey": "SA...",
    "secretKey": "SECRET...",
    "status": "Active"
  },
  "expiration": 1712345678
}
```

## Migration Process

To ensure a seamless transition for existing deployments, the IAM service includes an **automatic migration logic** that runs on startup.

### Trigger Logic
Migration is triggered **automatically** when the IAM service starts if:
1.  The legacy `/etc/iam/identity.json` file exists and contains user, account or service account definitions.
2.  The new `/etc/iam/users/`, `/etc/iam/accounts/` OR `/etc/iam/service_accounts/` directories are empty.

### Migration Steps
The migration process performs the following steps:

1.  **Read Legacy Config:** The service loads the full `identity.json`.
2.  **Extract Entities:** It iterates through all identities, accounts and service accounts defined in the monolithic file.
3.  **Create Individual Files:**
    *   For each identity, it writes a `<username>.json` to `/etc/iam/users/`.
    *   For each account, it writes a `<account_id>.json` to `/etc/iam/accounts/`.
    *   For each service account, it writes a `<service_account_id>.json` to `/etc/iam/service_accounts/`.

> **Note:** The migration process is **non-destructive**. The original `identity.json` file is left unchanged. This ensures that if something goes wrong with the individual files, the original configuration is still intact. However, for the system to fully utilize the scalable storage and avoid potential ambiguity, it is recommended to manually update `identity.json` after successful verification.

### Recovery & Rollback
*   **Rollback:** Since the original `identity.json` is preserved, rolling back simply deals with deleting the generated files in `/etc/iam/users/` (assuming no new users were created that need to be preserved).
*   **Conflict Resolution:** If individual files already exist in `/etc/iam/users/`, the migration completely skips the process to prevent overwriting existing data.

## Operational Changes

*   **User Management:** Creating, updating, or deleting a user now maps directly to creating, updating, or deleting a file in `/etc/iam/users/`.
*   **Listing Users:** Listing users performs a directory listing on `/etc/iam/users/` rather than parsing a large JSON blob.
*   **Access Key Lookup:** Looking up a user by Access Key performs a paginated scan of the user directory. Note that the legacy `identity.json` is checked *first* for backward compatibility, so it is important to clean up the legacy file to ensure new changes take precedence.
