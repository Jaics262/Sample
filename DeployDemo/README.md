# Umbraco Deploy demo

Umbraco 17.4.2 on .NET 10. Same seeded content as the TimeMachine and uRestore demos, with **Umbraco Deploy On-premises** installed for environment-to-environment transfer of schema, content, and media.

**Welcome** and **Office hours** are simple note pages. **Delivery questions** is an FAQ page with a nested Block List so Block List deploy via `Umbraco.Deploy.Contrib` can be exercised once a second workspace is available.

## Important

Deploy is a **commercial** product. This project installs the packages and workspace config so you can prove what appears in Settings. Full transfer between environments needs a valid license key and at least two reachable workspaces.

Set your key in `appsettings.json`:

```json
"Umbraco": {
  "Licenses": {
    "Products": {
      "Umbraco.Deploy.OnPrem": "YOUR-LICENSE-KEY"
    }
  }
}
```

Without a license the site still boots; Deploy UI shows as unlicensed.

## Run

```bash
dotnet run --project DeployDemo --urls https://localhost:5143
```

Open https://localhost:5143/umbraco

- Email: `admin@example.com`
- Password: `Deploy-demo-1`

## What to open

1. Sign in to the backoffice.
2. Open **Settings** and look for the Deploy / Workspaces dashboard (label depends on license state).
3. Confirm `Umbraco:Deploy:Project:Workspaces` lists Development (`https://localhost:5143`) and Live (`https://localhost:5144`).
4. Open **Delivery questions** and use normal **Info → Rollback** if you need version history on one page — Deploy does **not** replace that screen.

## What this is / is not

| Need | Tool |
| --- | --- |
| Move schema/content between environments | **Umbraco Deploy** (this project) |
| Site-wide change timeline + rollback | **uSync TimeMachine** (`TimeMachineDemo`) |
| Restore selected properties on one page | **uRestore** (`URestoreDemo`) |
| Visual old-page preview | **Rollback Previewer** (`BlockDiffDemo`) |

Deploy compares and transfers **environments**, not older versions of one document.

## Packages

- `Umbraco.Cms` 17.4.2
- `Umbraco.Deploy.OnPrem` 17.3.1
- `Umbraco.Deploy.Contrib` 17.0.0 (Block List / Block Grid transfer support)

The first start installs Umbraco and seeds the content. Later starts reuse `umbraco/Data`. Delete that folder to seed again.
