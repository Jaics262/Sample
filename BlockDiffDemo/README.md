# Block diff demo

Umbraco 17.4.2 on .NET 10. Home has a Block List with a nested Block List and three saved versions. About has only text fields and two versions. The **Block diff** tab on a content item shows those values as fields.

## Run

```bash
dotnet run --urls https://localhost:5123
```

Open https://localhost:5123/umbraco

The backoffice sign-in requires HTTPS.

- Email: `admin@example.com`
- Password: `BlockDiff-demo-1`

Open **Home** or **About**, then the **Block diff** tab. The selectors start on the oldest and newest versions.

**Info → Rollback** opens Rollback Previewer. The visual tab renders the page template for the current version and the selected version. Share is enabled for 60 minutes. The static demo secret, used when time-limited links are turned off, is `block-diff-share-demo`.

The first start installs Umbraco and seeds the content. Later starts reuse the SQLite database in `umbraco/Data`. Delete that folder to seed again.
