# TimeMachine demo

Umbraco 17.4.2 on .NET 10. Home-style note pages plus an FAQ with nested Block List content and several saved versions. **uSync TimeMachine** records timeline snapshots so you can compare and roll content back from the TimeMachine UI.

## Run

```bash
dotnet run --project TimeMachineDemo --urls https://localhost:44376
```

Open https://localhost:44376/umbraco

The backoffice sign-in requires HTTPS.

- Email: `admin@example.com`
- Password: `TimeMachine-demo-1`

## Try the timeline

1. Open **Delivery questions** or **Welcome**.
2. Use the **TimeMachine** UI (from uSync TimeMachine) to browse timeline snapshots.
3. Compare an earlier snapshot with the current content, then restore if needed.

**Info → Rollback** remains the built-in CMS version history for a single item.

## Seeded content

| Page | Useful for |
| --- | --- |
| Welcome / Office hours | Simple text changes across versions |
| Delivery questions | Nested accordion Block List with multiple saves |

TimeMachine config lives under `uSync:TimeMachine` in `appsettings.Development.json` (`Enabled`, `RetentionDays`).

The first start installs Umbraco and seeds the content. Later starts reuse the SQLite database in `umbraco/Data`. Delete that folder to seed again.

## Packages

- `Umbraco.Cms` 17.4.2
- `uSync` 17.4.3
- `uSync.TimeMachine` / `uSync.TimeMachine.Client` 17.5.0

For property-level restore without TimeMachine, see [URestoreDemo](../URestoreDemo/README.md).
