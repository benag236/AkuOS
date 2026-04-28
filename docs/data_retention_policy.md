# AkuOS Data Retention and Deletion Policy

Last updated: April 28, 2026

## Purpose and Scope

This policy explains how AkuOS retains, removes, and deletes customer financial data in a controlled, compliance-oriented way.

It applies to AkuOS account data, financial records, connected account metadata, transactions, rules, merchant memory, budgets, goals, subscriptions, imports, preferences, logs, backups, and related operational records.

## Retention Principle

AkuOS stores customer data only as long as necessary to provide the service, operate the app, support security needs, meet compliance-oriented obligations, or resolve user requests.

Data should not be retained indefinitely when it is no longer needed for a legitimate product, operational, security, legal, or compliance-oriented purpose.

## User Deletion Requests

Users may request deletion of their AkuOS financial data at any time by contacting the published AkuOS contact email.

Deletion requests are reviewed and processed in a reasonable timeframe. Some operational records may be retained if necessary for security, fraud prevention, legal compliance, dispute handling, or audit purposes.

## Plaid Credentials

AkuOS does not store Plaid bank login credentials. Bank credential collection is handled by Plaid during the connection flow.

AkuOS may store Plaid-related access tokens, item identifiers, account metadata, and transaction data needed to provide connected account functionality, subject to this retention policy.

## Inactive and Unused Data

Data associated with inactive accounts, stale imports, disconnected integrations, or unused operational records may be periodically reviewed and removed when no longer necessary.

Unused data may be periodically removed to reduce unnecessary retention and limit exposure.

## Secure Deletion Procedures

AkuOS deletion procedures should be performed through controlled application or administrative workflows.

Secure deletion practices include:

- Deleting user-scoped financial records from active application storage when a deletion request is completed.
- Removing or invalidating connected account tokens and integration records where applicable.
- Removing generated import previews, temporary files, and operational artifacts when they are no longer needed.
- Avoiding deletion logs that expose secrets, bank credentials, or unnecessary sensitive financial details.
- Allowing provider-managed backups and retained copies to expire according to applicable infrastructure retention cycles.

## Compliance-Oriented Handling

AkuOS aims to keep data retention limited, documented, and purpose-based.

Retention and deletion practices may be adjusted to support applicable legal, regulatory, fraud prevention, audit, security, or operational obligations.

## Deletion Verification

Deletion work should be reviewed for completion where practical. Verification may include confirming that active application records, connected account references, import artifacts, and user-scoped financial records have been removed or invalidated as applicable.

## Related Policies

This policy complements the AkuOS Privacy Policy, Information Security Policy, and Access Control Policy.

## Contact

Retention questions or deletion requests can be sent to the published AkuOS contact email.
