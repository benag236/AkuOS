# Security Policy

## Supported Project

AkuOS dependency and vulnerability management applies to the current production branch and actively maintained deployment configuration.

## Vulnerability Management

AkuOS uses automated dependency monitoring and audit checks to identify known vulnerabilities in application dependencies.

- Dependabot checks Python dependencies and GitHub Actions on a weekly schedule.
- GitHub Actions runs `pip-audit` against `requirements.txt`.
- GitHub Actions runs `npm audit --audit-level=high` when a frontend `package.json` exists.
- Dependencies are reviewed and updated regularly.

## Remediation Targets

AkuOS uses the following remediation targets once a vulnerability is confirmed and applicable:

- Critical severity vulnerabilities: patched within 7 days.
- High severity vulnerabilities: patched within 30 days.
- Medium and low severity vulnerabilities: reviewed during regular dependency maintenance.

## Reporting a Vulnerability

Report suspected vulnerabilities privately to the project owner or published AkuOS security contact.

Please include:

- A description of the issue.
- Steps to reproduce, if available.
- Affected routes, dependencies, or configuration.
- Any relevant logs or screenshots that do not expose secrets or customer financial data.

## Security Practices

AkuOS stores production secrets in environment variables, avoids committing production credentials to source code, and relies on provider-level controls for hosted infrastructure.

Production deployments must provide `SECRET_KEY`, Plaid credentials, database URLs, and token encryption material through environment variables. Production startup fails if `SECRET_KEY` is missing.

AkuOS enforces HTTPS redirects in production, sets secure session cookie flags, and applies browser security headers including HSTS, content type protection, frame protection, referrer policy, permissions policy, and a content security policy compatible with the current CDN and Plaid Link dependencies.

Do not include bank credentials, API secrets, database URLs, session secrets, or customer financial data in vulnerability reports unless explicitly requested through a secure channel.

## Plaid Token Handling

Plaid bank login credentials are handled by Plaid and are not stored by AkuOS.

Plaid access tokens are encrypted before being written to the database. Token encryption uses `PLAID_TOKEN_ENCRYPTION_KEY` when set, with `SECRET_KEY` as the fallback key material. Both values must come from environment variables in production.

Plaid tokens, API secrets, public tokens, and client secrets must not be logged, committed to source control, exposed in templates, or included in exports.
