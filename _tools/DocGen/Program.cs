using DocumentFormat.OpenXml;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Wordprocessing;
using A = DocumentFormat.OpenXml.Drawing;
using DW = DocumentFormat.OpenXml.Drawing.Wordprocessing;
using PIC = DocumentFormat.OpenXml.Drawing.Pictures;

var target = args.Length > 0 ? args[0] : "deploy";
if (target.Equals("urestore", StringComparison.OrdinalIgnoreCase))
{
    WriteURestore();
}
else
{
    WriteDeploy();
}

static void WriteDeploy()
{
    var docsRoot = ResolveDocs("umbraco-deploy");
    var output = Path.Combine(docsRoot, "Umbraco-Deploy.docx");
    var images = new[]
    {
        (Path: Path.Combine(docsRoot, "settings-deploy.png"), Caption: "Settings → Deploy dashboard. Version 17.3.1 reports Ready, with schema update operations from .uda files."),
        (Path: Path.Combine(docsRoot, "workspaces-config.png"), Caption: "Same dashboard with the Development workspace badge in the header (CurrentWorkspaceName from appsettings)."),
        (Path: Path.Combine(docsRoot, "faq-page.png"), Caption: "The seeded FAQ page used as sample content for transfer proofs once a second environment exists."),
    };

    WriteDocument(
        output,
        images,
        title: "Umbraco Deploy On-premises on Umbraco 17",
        intro: "Proof notes from the DeployDemo site. Checked on 13 October 2026.",
        buildBody: body =>
        {
            body.Append(Heading("Overview"));
            body.Append(Para("Umbraco Deploy On-premises is the same deployment engine used on Umbraco Cloud, packaged for self-hosted sites. It transfers schema, content, and media between named workspaces (for example Development, Staging, Live). It is not a version-history viewer for a single content item."));
            body.Append(Para("NuGet packages on this proof: Umbraco.Deploy.OnPrem 17.3.1 and Umbraco.Deploy.Contrib 17.0.0 (Block List / Block Grid transfer helpers). Both target .NET 10 with Umbraco CMS 17."));
            body.Append(Para("Deploy is commercial. Public pricing checked earlier pointed to about €2,800 as a subscription figure from 1 January 2026. Confirm the current price and billing interval with Umbraco before budgeting. A valid licence key is required for full workspace transfer."));

            body.Append(Heading("Detail"));
            body.Append(Heading2("What this proof installed"));
            body.Append(Para("Project: DeployDemo, in POC beside TimeMachineDemo, URestoreDemo, and BlockDiffDemo. .NET 10."));
            body.Append(Para("Umbraco.Cms 17.4.2."));
            body.Append(Para("Umbraco.Deploy.OnPrem 17.3.1 and Umbraco.Deploy.Contrib 17.0.0."));
            body.Append(Para("SQLite, unattended install. The site runs at https://localhost:5143/umbraco. Admin credentials are in appsettings.Development.json (admin@example.com / Deploy-demo-1)."));

            body.Append(Heading2("What it moves"));
            body.Append(Para("Deploy serialises schema and content into .uda files under umbraco/Deploy and pushes or pulls between workspaces over HTTPS using a shared ApiSecret. Editors use the Workspaces dashboard in Settings, not Info → Rollback on a single page."));
            body.Append(Para("Unlike uSync TimeMachine, there is no site-wide timeline of backoffice edits with property diffs. Unlike uRestore, there is no selective property restore from an older version of the same node. Deploy answers: take what is on this environment and put it on that environment."));

            body.Append(Heading2("The sample content"));
            body.Append(Para("Delivery questions is an FAQ page with nested Accordion Block Lists. Welcome and Office hours are simple note pages. The same seed exists on TimeMachineDemo and URestoreDemo so you can compare tools against identical content."));

            body.Append(Heading2("Configuration"));
            body.Append(Para("Umbraco:Licenses:Products:Umbraco.Deploy.OnPrem holds the licence key (placeholder REPLACE-WITH-YOUR-DEPLOY-LICENSE-KEY on this proof)."));
            body.Append(Para("Umbraco:Deploy:Settings:ApiSecret is a 64-byte Base64 secret shared by every environment in the project."));
            body.Append(Para("Umbraco:Deploy:Project:CurrentWorkspaceName is Development. Workspaces list Development at https://localhost:5143 and Live at https://localhost:5144 as a second environment placeholder."));

            body.Append(Heading2("Licence note for this proof"));
            body.Append(Para("This repository does not include a paid Deploy licence. The project is meant to show install, configuration, and the boundary of the product. Replace the placeholder key and stand up a second workspace URL before proving a real transfer."));

            body.Append(Heading("Pros"));
            body.Append(Bullet("Same engine as Umbraco Cloud for reliable schema and content transfer between self-hosted environments."));
            body.Append(Bullet("Contrib package covers Block List / Block Grid content editors used on modern sites."));
            body.Append(Bullet("Fits CI/CD workflows with .uda files in source control."));
            body.Append(Bullet("Runs on Umbraco 17 / .NET 10."));

            body.Append(Heading("Cons"));
            body.Append(Bullet("Paid product. Not a free substitute for Rollback Previewer, uRestore, or TimeMachine."));
            body.Append(Bullet("Needs a licence key and at least two reachable HTTPS environments to prove transfer."));
            body.Append(Bullet("Does not show readable nested block field diffs for an older version of one page."));
            body.Append(Bullet("Does not provide a visual old-page preview or share link."));
            body.Append(Bullet("Not a content-version history tool. Info → Rollback remains separate."));

            body.Append(Heading("Screenshots"));
            body.Append(Para("Taken from DeployDemo on 13 October 2026 (or product documentation placeholders if the licence UI blocked a live capture)."));
        },
        closing: "Add a real Deploy licence key, bring up a second environment on the Live URL, then transfer Delivery questions. Compare that outcome with TimeMachineDemo (timeline) and URestoreDemo (property restore) to confirm Deploy is the environment-transfer tool, not the history viewer.");
}

static void WriteURestore()
{
    var docsRoot = ResolveDocs("urestore");
    var output = Path.Combine(docsRoot, "uRestore.docx");
    var images = new[]
    {
        (Path: Path.Combine(docsRoot, "property-restore-versions.png"), Caption: "Property Restore tab. Version list with culture filter and draft, published, and saved states."),
        (Path: Path.Combine(docsRoot, "property-restore-diff.png"), Caption: "Property diff. Changed properties are pre-selected. Media and rich text values still appear as JSON or markup."),
        (Path: Path.Combine(docsRoot, "faq-page.png"), Caption: "The published FAQ page (Delivery questions) that those content versions produced."),
    };

    WriteDocument(
        output,
        images,
        title: "uRestore on Umbraco 17",
        intro: "Proof notes from the URestoreDemo site. Checked on 13 October 2026.",
        buildBody: body =>
        {
            body.Append(Heading("Overview"));
            body.Append(Para("uRestore is a backoffice Property Restore workspace view. On a content item it lists saved versions, diffs each property against the current draft, and lets an editor restore only the properties they select."));
            body.Append(Para("NuGet package: Umbraco.Community.uRestore 1.0.7 (MIT)."));
        },
        closing: "Open Delivery questions, then Property Restore.");
}

static string ResolveDocs(string folder)
{
    var docsRoot = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..", "docs", folder));
    if (!Directory.Exists(docsRoot))
    {
        docsRoot = Path.Combine(@"C:\Projects\POC\docs", folder);
    }

    Directory.CreateDirectory(docsRoot);
    return docsRoot;
}

static void WriteDocument(
    string output,
    (string Path, string Caption)[] images,
    string title,
    string intro,
    Action<Body> buildBody,
    string closing)
{
    foreach (var image in images)
    {
        if (!File.Exists(image.Path))
        {
            throw new FileNotFoundException("Screenshot missing.", image.Path);
        }
    }

    if (File.Exists(output))
    {
        File.Delete(output);
    }

    using var document = WordprocessingDocument.Create(output, WordprocessingDocumentType.Document);
    var mainPart = document.AddMainDocumentPart();
    mainPart.Document = new Document(new Body());
    var body = mainPart.Document.Body!;

    body.Append(Title(title));
    body.Append(Para(intro));
    buildBody(body);

    var imageId = 1U;
    foreach (var image in images)
    {
        AddImage(mainPart, body, image.Path, image.Caption, ref imageId);
    }

    body.Append(Heading("Where to look next"));
    body.Append(Para(closing));
    mainPart.Document.Save();
    Console.WriteLine($"Wrote {output}");
}

static Paragraph Title(string text) => new(
    new ParagraphProperties(new SpacingBetweenLines { After = "200" }),
    new Run(new RunProperties(new Bold(), new FontSize { Val = "36" }), new Text(text)));

static Paragraph Heading(string text) => new(
    new ParagraphProperties(new SpacingBetweenLines { Before = "360", After = "120" }),
    new Run(new RunProperties(new Bold(), new FontSize { Val = "28" }), new Text(text)));

static Paragraph Heading2(string text) => new(
    new ParagraphProperties(new SpacingBetweenLines { Before = "240", After = "80" }),
    new Run(new RunProperties(new Bold(), new FontSize { Val = "24" }), new Text(text)));

static Paragraph Para(string text) => new(
    new ParagraphProperties(new SpacingBetweenLines { After = "160" }),
    new Run(new RunProperties(new FontSize { Val = "22" }), new Text(text) { Space = SpaceProcessingModeValues.Preserve }));

static Paragraph Bullet(string text) => new(
    new ParagraphProperties(
        new SpacingBetweenLines { After = "80" },
        new Indentation { Left = "360" }),
    new Run(new RunProperties(new FontSize { Val = "22" }), new Text("• " + text) { Space = SpaceProcessingModeValues.Preserve }));

static void AddImage(MainDocumentPart mainPart, Body body, string path, string caption, ref uint imageId)
{
    var imagePart = mainPart.AddImagePart(ImagePartType.Png);
    using (var stream = File.OpenRead(path))
    {
        imagePart.FeedData(stream);
    }

    var relId = mainPart.GetIdOfPart(imagePart);
    long widthEmu;
    long heightEmu;
    using (var fs = File.OpenRead(path))
    using (var img = System.Drawing.Image.FromStream(fs, false, false))
    {
        const int maxWidthPx = 520;
        var scale = img.Width > maxWidthPx ? (double)maxWidthPx / img.Width : 1.0;
        widthEmu = (long)(img.Width * scale * 9525);
        heightEmu = (long)(img.Height * scale * 9525);
    }

    var element = new Drawing(
        new DW.Inline(
            new DW.Extent { Cx = widthEmu, Cy = heightEmu },
            new DW.EffectExtent { LeftEdge = 0L, TopEdge = 0L, RightEdge = 0L, BottomEdge = 0L },
            new DW.DocProperties { Id = imageId, Name = Path.GetFileName(path) },
            new DW.NonVisualGraphicFrameDrawingProperties(new A.GraphicFrameLocks { NoChangeAspect = true }),
            new A.Graphic(
                new A.GraphicData(
                    new PIC.Picture(
                        new PIC.NonVisualPictureProperties(
                            new PIC.NonVisualDrawingProperties { Id = 0U, Name = Path.GetFileName(path) },
                            new PIC.NonVisualPictureDrawingProperties()),
                        new PIC.BlipFill(
                            new A.Blip { Embed = relId, CompressionState = A.BlipCompressionValues.Print },
                            new A.Stretch(new A.FillRectangle())),
                        new PIC.ShapeProperties(
                            new A.Transform2D(
                                new A.Offset { X = 0L, Y = 0L },
                                new A.Extents { Cx = widthEmu, Cy = heightEmu }),
                            new A.PresetGeometry(new A.AdjustValueList()) { Preset = A.ShapeTypeValues.Rectangle }))
                )
                { Uri = "http://schemas.openxmlformats.org/drawingml/2006/picture" })
        )
        {
            DistanceFromTop = 0U,
            DistanceFromBottom = 0U,
            DistanceFromLeft = 0U,
            DistanceFromRight = 0U,
        });

    body.Append(new Paragraph(new Run(element)));
    body.Append(new Paragraph(
        new ParagraphProperties(new SpacingBetweenLines { After = "240" }),
        new Run(new RunProperties(new Italic(), new FontSize { Val = "20" }), new Text(caption) { Space = SpaceProcessingModeValues.Preserve })));
    imageId++;
}
