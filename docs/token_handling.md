# AkuOS Token Handling Practices

Last updated: April 28, 2026

## Purpose

This document describes how AkuOS handles sensitive tokens used for connected financial accounts and application security.

## Environment-Provided Secrets

Production secrets must be loaded from environment variables. Required or sensitive values include:

- `SECRET_KEY`
- `PLAID_TOKEN_ENCRYPTION_KEY`
- `PLAID_CLIENT_ID`
- `PLAID_SECRET`
- `DATABASE_URL`

Production credentials must not be committed to source code, stored in templates, or printed in logs.

## Plaid Credentials

AkuOS does not store bank usernames or passwords. Bank credential collection is handled by Plaid during the bank connection flow.

AkuOS receives Plaid public tokens from Plaid Link, exchanges them server-side, and stores only the resulting access token needed for account sync.

## Plaid Access Tokens

Plaid access tokens are encrypted before database storage.

Token encryption uses `PLAID_TOKEN_ENCRYPTION_KEY` when configured. If that value is not configured, AkuOS uses the environment-provided `SECRET_KEY` as key material. The derived encryption key is used only server-side.

Existing plaintext Plaid tokens are migrated to encrypted storage when the app initializes token protection.

## Logging and Exposure Controls

Plaid access tokens, public tokens, API secrets, session secrets, database URLs, and raw credentials must not be intentionally logged.

Error messages and operational logs should include enough context to troubleshoot without exposing secret values or customer financial data.

## Rotation and Revocation

If token exposure is suspected, affected tokens should be revoked or rotated where supported, application secrets should be rotated, and impacted users should be reviewed for follow-up.
