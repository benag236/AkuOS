# AkuOS Information Security Policy

Last updated: April 28, 2026

## Purpose and Scope

This policy describes the security practices used to protect AkuOS customer financial data, application systems, hosted infrastructure, third-party integrations, and administrative operations.

It applies to AkuOS application code, production configuration, connected service providers, financial data handling, logging, monitoring, access control, and incident response practices.

## Protection of Customer Financial Data

AkuOS uses customer financial data only to provide app functionality such as dashboards, transactions, accounts, imports, budgets, rules, merchant memory, goals, subscriptions, and account sync.

Customer financial records should remain scoped to the signed-in user. Passwords, bank credentials, production secrets, and API keys must not be included in user exports or intentionally exposed in logs.

## HTTPS and TLS Encryption

AkuOS is intended to be served over HTTPS/TLS in production. HTTPS/TLS protects data in transit between the user, AkuOS, and supported third-party providers.

## Environment Variable Handling for Secrets

Production secrets must be provided through environment variables. This includes session secrets, database URLs, encryption keys, Plaid credentials, and other provider secrets.

Secrets must not be hardcoded in source code, committed to version control, printed in logs, or included in client-side templates.

## Principle of Least Privilege

Access to AkuOS systems, data, infrastructure, and configuration should be limited to the minimum access required to operate and support the service.

Administrative access should be restricted to authorized users and reviewed as the app grows.

## Access Control Practices

AkuOS requires authentication for user financial workflows. Financial data queries and mutations should be scoped to the current user.

Administrative and maintenance actions should be protected from public access and limited to authorized operators.

## Secure Third-Party Providers

AkuOS uses Plaid for bank connections and Render for hosting.

Plaid handles bank credential collection during connection flows. AkuOS does not store bank usernames or passwords. Render provides hosted infrastructure controls and platform-level security protections.

## Logging and Monitoring Practices

Application logs should support troubleshooting, operational monitoring, and security review.

Logs should not intentionally include bank credentials, production secrets, raw API secrets, or unnecessary sensitive financial details. Security-relevant errors and rejected requests should be logged with enough context to investigate without exposing secrets.

## Dependency Update Practices

Application dependencies should be reviewed and updated regularly so security fixes, compatibility updates, and maintenance patches are applied in a timely way.

Dependency changes should be tested before production deployment when practical.

## Incident Response Statement

If a security incident is suspected, AkuOS will investigate the issue, reduce exposure, preserve relevant logs where appropriate, and communicate with affected users when required.

Reported security concerns should be sent to the published contact email.

## Data Retention and Deletion

AkuOS stores data only as long as necessary to provide the service. Users can request deletion at any time, and unused data may be periodically removed.

See the AkuOS Privacy Policy for additional data retention and deletion details.
