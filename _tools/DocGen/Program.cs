using DocumentFormat.OpenXml;
using DocumentFormat.OpenXml.Packaging;
using DocumentFormat.OpenXml.Wordprocessing;
using A = DocumentFormat.OpenXml.Drawing;
using DW = DocumentFormat.OpenXml.Drawing.Wordprocessing;
using PIC = DocumentFormat.OpenXml.Drawing.Pictures;

var docsRoot = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", "..", "docs", "urestore"));
if (!Directory.Exists(docsRoot))
{
    docsRoot = @"C:\Projects\POC\docs\urestore";
}

var output = Path.Combine(docsRoot, "uRestore.docx");
var images = new[]
{
    (Path: Path.Combine(docsRoot, "property-restore-versions.png"), Caption: "Property Restore tab. Version list with culture filter and draft, published, and saved states."),
    (Path: Path.Combine(docsRoot, "property-restore-diff.png"), Caption: "Property diff. Changed properties are pre-selected. Media and rich text values still appear as JSON or markup."),
    (Path: Path.Combine(docsRoot, "faq-page.png"), Caption: "The published FAQ page (Delivery questions) that those content versions produced."),
};

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

using (var document = WordprocessingDocument.Create(output, WordprocessingDocumentType.Document))
{
    var mainPart = document.AddMainDocumentPart();
    mainPart.Document = new Document(new Body());
    var body = mainPart.Document.Body!;

    body.Append(Title("uRestore on Umbraco 17"));
    body.Append(Para("Proof notes from the URestoreDemo site. Checked on 13 October 2026."));

    body.Append(Heading("Overview"));
    body.Append(Para("uRestore is a backoffice Property Restore workspace view. On a content item it lists saved versions, diffs each property against the current draft, and lets an editor restore only the properties they select. Restored values become a new draft, or save-and-publish from the confirmation dialog."));
    body.Append(Para("NuGet package: Umbraco.Community.uRestore 1.0.7 (MIT). It targets .NET 10 and Umbraco 17+. It does not replace Info → Rollback. There is no site-wide timeline. Restore is always scoped to the open content node."));
    body.Append(Para("No appsettings section is required. After install and restart, the Property Restore tab appears on content workspaces."));

    body.Append(Heading("Detail"));
    body.Append(Heading2("What this proof installed"));
    body.Append(Para("Project: URestoreDemo, in POC beside TimeMachineDemo and BlockDiffDemo. .NET 10."));
    body.Append(Para("Umbraco.Cms 17.4.2."));
    body.Append(Para("Umbraco.Community.uRestore 1.0.7."));
    body.Append(Para("SQLite, unattended install. The site runs at https://localhost:5133/umbraco. The admin user is in appsettings.Development.json (admin@example.com / URestore-demo-1)."));

    body.Append(Heading2("What it restores"));
    body.Append(Para("uRestore works from Umbraco content version history for the current item. Editors filter by culture on multilingual sites. Changed properties are pre-selected. Properties that match the current draft stay unchecked."));
    body.Append(Para("Unlike uSync TimeMachine, there is no common timeline for many content items, document types, or data types. Welcome, Office hours, and Delivery questions each have their own Property Restore tab."));

    body.Append(Heading2("The sample content"));
    body.Append(Para("Delivery questions is an FAQ page. Accordions is a Block List of Accordion blocks. Each accordion has its own Block List of Accordion item blocks, with a heading and text. The page was saved several times so the version list has multiple entries: Shipping created, Returns added, item text edited, an item removed, titles renamed, and a final publish."));
    body.Append(Para("Welcome and Office hours are simple note pages for proving a single text-field restore without touching other properties."));

    body.Append(Heading2("How a block change is shown"));
    body.Append(Para("Non-string values are serialised with JsonSerializer.Serialize. Block List, Block Grid, Rich Text, and media pickers therefore still show JSON or raw markup in the property diff."));
    body.Append(Para("A Block List or Block Grid restores as the whole property, not as one nested block or field. Selecting Accordions puts the entire nested structure back. This matches the package documentation and is the same limit as the built-in Rollback screen for nested content."));

    body.Append(Heading2("Restore"));
    body.Append(Para("Compare opens the property diff. Restore selected asks for Save as draft or Save & Publish. Draft restore is non-destructive: it does not auto-publish. Info → Rollback remains available for whole-version rollback."));

    body.Append(Heading2("Configuration"));
    body.Append(Para("None. ContentVersionCleanupPolicy:EnableCleanup is false in the Development appsettings so the seeded versions are not pruned during the proof."));

    body.Append(Heading("Pros"));
    body.Append(Bullet("Selective property restore on one page without overwriting other fields."));
    body.Append(Bullet("Free MIT package on Umbraco 17 / .NET 10."));
    body.Append(Bullet("Leaves the built-in Rollback screen in place."));
    body.Append(Bullet("Culture filter for variants."));
    body.Append(Bullet("Changed properties are pre-selected, which speeds up simple text restores."));
    body.Append(Bullet("Same seeded FAQ content as TimeMachineDemo, so the two proofs can be compared side by side."));

    body.Append(Heading("Cons"));
    body.Append(Bullet("No site-wide timeline. You cannot restore many content items from one screen the way TimeMachine does."));
    body.Append(Bullet("Block List and nested accordion changes are not readable as labelled fields. The property restores as one JSON value."));
    body.Append(Bullet("No visual page preview and no share link (use Rollback Previewer for that)."));
    body.Append(Bullet("Early package: low download count when checked. Treat as a trial before relying on it in production."));
    body.Append(Bullet("Does not restore media or member properties."));

    body.Append(Heading("Screenshots"));
    body.Append(Para("Taken on the URestoreDemo backoffice and public site on 13 October 2026."));

    var imageId = 1U;
    foreach (var image in images)
    {
        AddImage(mainPart, body, image.Path, image.Caption, ref imageId);
    }

    body.Append(Heading("Where to look next"));
    body.Append(Para("Open Delivery questions, then Property Restore. Compare an older version and restore only Title or Introduction to prove selective restore. Try restoring Accordions alone and confirm the nested blocks come back as one property. Compare that behaviour with Settings → uSync → TimeMachine on TimeMachineDemo if you need a common timeline for more content."));

    mainPart.Document.Save();
}

Console.WriteLine($"Wrote {output}");

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
