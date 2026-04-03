# Render Deployment Notes

This project is now prepared to run on Render with Gunicorn.

## Default Build And Start

- Build command: `pip install -r requirements.txt`
- Start command: `gunicorn app:app --bind 0.0.0.0:$PORT --workers 2 --timeout 120`

## Environment Variables

- `SECRET_KEY`
  - Required in production.
  - Render can generate this automatically from `render.yaml`.
- `DATABASE_URL`
  - Recommended for production.
  - If set, the app will use it.
  - `postgres://...` URLs are normalized automatically to `postgresql://...`.

## Database Behavior

- Local development:
  - Falls back to `finance.db` in the project directory.
- Render production:
  - Best option: set `DATABASE_URL` to a managed Postgres database.
  - Fallback option: attach a persistent disk and set `RENDER_DISK_PATH`; the app will place `finance.db` there if `DATABASE_URL` is not set.

## Recommended Render Setup

1. Create a new Web Service from this repo.
2. Use the values from `render.yaml`, or enter them manually.
3. Add a Postgres database and set `DATABASE_URL`.
4. Keep `SECRET_KEY` set to a long random value.

## Notes

- The app object for Gunicorn is `app:app`.
- `healthCheckPath` is set to `/login`.
- No app functionality was changed in this deployment-prep pass; this only makes config and startup production-friendly.
