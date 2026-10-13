# Umbraco Deploy proof notes

Word document and screenshots matching the style of `docs/usync-timemachine/uSync-TimeMachine.docx` and `docs/urestore/uRestore.docx`.

| File | Description |
| --- | --- |
| [Umbraco-Deploy.docx](Umbraco-Deploy.docx) | Proof notes for DeployDemo |
| [settings-deploy.png](settings-deploy.png) | Deploy status dashboard (v17.3.1, Ready) |
| [workspaces-config.png](workspaces-config.png) | Deploy dashboard with Development workspace badge |
| [faq-page.png](faq-page.png) | Live published FAQ page |

Regenerate the Word file:

```bash
dotnet run --project ../../_tools/DocGen -- deploy
```

Deploy is commercial. Replace `Umbraco:Licenses:Products:Umbraco.Deploy.OnPrem` with a real key before proving environment transfer.
