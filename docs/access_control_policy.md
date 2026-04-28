# AkuOS Access Control Policy

Last updated: April 28, 2026

## Purpose and Scope

This policy defines how AkuOS restricts administrative and production access, handles credentials, and reviews privileges.

It applies to AkuOS production systems, infrastructure accounts, administrative workflows, source code, secrets, and operational tools used to provide and maintain the service.

## Authorized Administrator Access

Only authorized administrators can access AkuOS production systems, infrastructure dashboards, deployment settings, databases, or production configuration.

Production access should be granted only when there is a legitimate operational, security, deployment, or support need.

## Least Privilege

Access is limited using least privilege principles. Administrators should receive only the permissions needed to perform their assigned responsibilities.

Permissions should be reduced or removed when they are no longer required.

## Secrets and Environment Variables

Secrets are stored in environment variables only. This includes production credentials, API keys, database URLs, encryption keys, Plaid secrets, and session secrets.

Secrets must not be printed in logs, exposed in templates, shared through unsecured channels, or included in exported user data.

## No Production Credentials in Source Code

No production credentials are stored in source code. Production API secrets, database URLs, encryption keys, session secrets, and provider credentials must not be committed to the AkuOS repository.

## Periodic Access Reviews

Access reviews are performed periodically to confirm that production, infrastructure, and administrative access remains appropriate.

Unneeded access should be removed when an administrator no longer requires it.

## Admin Authentication

Authentication is required for admin access. Administrative workflows, maintenance actions, and sensitive operational tasks should be restricted to authenticated and authorized users.

## Production Infrastructure Restrictions

Production infrastructure access is restricted to authorized administrators and should be managed through approved provider controls, such as Render account and service permissions.

Infrastructure access should not be shared through common accounts when individual administrator access is available.

## Monitoring and Change Awareness

Administrative changes and security-relevant access events should be logged or reviewed where practical.

Logs should not intentionally expose secrets, credentials, or unnecessary sensitive financial details.

## Policy Review

This policy should be reviewed as administrator workflows, hosting configuration, third-party providers, and production infrastructure evolve.
