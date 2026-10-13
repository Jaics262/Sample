# uRestore demo

Umbraco 17.4.2 on .NET 10. Same seeded content as the TimeMachine demo, but with **uRestore** instead of uSync TimeMachine.

**Welcome** and **Office hours** are simple note pages. **Delivery questions** is an FAQ page with a nested Block List (accordions → items) and several saved versions so you can compare property-level restore.

## Run

```bash
dotnet run --project URestoreDemo --urls https://localhost:5133
```

Open https://localhost:5133/umbraco

The backoffice sign-in requires HTTPS.

- Email: `admin@example.com`
- Password: `URestore-demo-1`

## Try property restore

1. Open **Delivery questions** (or **Welcome**).
2. Open the **Property Restore** tab (next to Content / Info).
3. Pick an older version and click **Compare**.
4. Review the property diffs. Changed properties are pre-selected.
5. Deselect anything you want to keep, then **Restore selected**.
6. Choose **Save as draft** or **Save & Publish**.

Built-in **Info → Rollback** is still available. uRestore does not replace it.

## What this shows

| Page | Useful for |
| --- | --- |
| Welcome / Office hours | Restoring a single text field without touching other properties |
| Delivery questions | Seeing that Block List restores as the **whole** property (JSON), not one nested block |

uRestore needs no extra `appsettings` section. After install, the Property Restore tab appears on content workspaces.

The first start installs Umbraco and seeds the content. Later starts reuse the SQLite database in `umbraco/Data`. Delete that folder to seed again.

## Packages

- `Umbraco.Cms` 17.4.2
- `Umbraco.Community.uRestore` 1.0.7

Compared with TimeMachineDemo: that project uses `uSync.TimeMachine` for timeline snapshots. This project uses uRestore for selective property restore from content versions.
