using System.Text.Json.Nodes;
using Umbraco.Cms.Core;
using Umbraco.Cms.Core.Events;
using Umbraco.Cms.Core.Models;
using Umbraco.Cms.Core.Notifications;
using Umbraco.Cms.Core.PropertyEditors;
using Umbraco.Cms.Core.Serialization;
using Umbraco.Cms.Core.Services;
using Umbraco.Cms.Core.Strings;

namespace BlockDiffDemo.Demo;

public class DemoContentSeeder : INotificationAsyncHandler<UmbracoApplicationStartedNotification>
{
    private readonly IContentService _contentService;
    private readonly IContentTypeService _contentTypeService;
    private readonly IDataTypeService _dataTypeService;
    private readonly IShortStringHelper _shortStringHelper;
    private readonly PropertyEditorCollection _propertyEditors;
    private readonly IConfigurationEditorJsonSerializer _serializer;
    private readonly ILogger<DemoContentSeeder> _logger;

    public DemoContentSeeder(
        IContentService contentService,
        IContentTypeService contentTypeService,
        IDataTypeService dataTypeService,
        IShortStringHelper shortStringHelper,
        PropertyEditorCollection propertyEditors,
        IConfigurationEditorJsonSerializer serializer,
        ILogger<DemoContentSeeder> logger)
    {
        _contentService = contentService;
        _contentTypeService = contentTypeService;
        _dataTypeService = dataTypeService;
        _shortStringHelper = shortStringHelper;
        _propertyEditors = propertyEditors;
        _serializer = serializer;
        _logger = logger;
    }

    public async Task HandleAsync(UmbracoApplicationStartedNotification notification, CancellationToken cancellationToken)
    {
        if (_contentTypeService.Get("landingPage") is not null)
        {
            return;
        }

        try
        {
            var text = PickDataType(Constants.PropertyEditors.Aliases.TextBox, "Textstring", "Text Box");
            var area = PickDataType(Constants.PropertyEditors.Aliases.TextArea, "Textarea", "Text Area");
            var toggle = PickDataType(Constants.PropertyEditors.Aliases.Boolean, "True/false", "Toggle");

            var highlight = CreateElement("highlightBlock", "Highlight", "icon-certificate");
            AddProperty(highlight, text, "title", "Title", 0);
            _contentTypeService.Save(highlight);

            var feature = CreateElement("featureBlock", "Feature", "icon-box");
            AddProperty(feature, text, "heading", "Heading", 0);
            AddProperty(feature, area, "text", "Text", 1);
            _contentTypeService.Save(feature);

            var quote = CreateElement("quoteBlock", "Quote", "icon-quote");
            AddProperty(quote, area, "quote", "Quote", 0);
            AddProperty(quote, text, "attribution", "Attribution", 1);
            _contentTypeService.Save(quote);

            var highlightsDataType = CreateBlockList("Highlights", highlight.Key);
            var hero = CreateElement("heroBlock", "Hero", "icon-picture");
            AddProperty(hero, text, "heading", "Heading", 0);
            AddProperty(hero, area, "text", "Text", 1);
            AddProperty(hero, highlightsDataType, "highlights", "Highlights", 2);
            _contentTypeService.Save(hero);

            var sectionsDataType = CreateBlockList("Page sections", hero.Key, feature.Key, quote.Key);
            var landing = CreateDocument("landingPage", "Landing page", "icon-home");
            AddProperty(landing, text, "title", "Title", 0);
            AddProperty(landing, area, "intro", "Introduction", 1);
            AddProperty(landing, toggle, "showBanner", "Show banner", 2);
            AddProperty(landing, sectionsDataType, "sections", "Page sections", 3);
            _contentTypeService.Save(landing);

            var about = CreateDocument("aboutPage", "About page", "icon-document");
            AddProperty(about, text, "title", "Title", 0);
            AddProperty(about, area, "summary", "Summary", 1);
            AddProperty(about, area, "body", "Body", 2);
            _contentTypeService.Save(about);

            await SeedHomeAsync(hero.Key, feature.Key, quote.Key, highlight.Key, cancellationToken);
            await SeedAboutAsync(cancellationToken);
            _logger.LogInformation("Seeded Home and About with multiple content versions.");
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "Demo content was not seeded. Delete umbraco/Data and start again after fixing the error.");
        }
    }

    private async Task SeedHomeAsync(Guid heroType, Guid featureType, Guid quoteType, Guid highlightType, CancellationToken cancellationToken)
    {
        var hero = Guid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1");
        var delivery = Guid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa2");
        var colours = Guid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa3");
        var quote = Guid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa4");
        var events = Guid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa5");
        var range = Guid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa6");
        var workshops = Guid.Parse("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa7");

        var home = _contentService.Create("Home", Constants.System.Root, "landingPage");
        ApplyHome(home, "Summer launch", "A short introduction to the summer campaign.", true, BlockList(
            Seed(heroType, hero, ("heading", Text("Welcome")), ("text", Text("Our summer range is here.")), ("highlights", BlockList(
                Seed(highlightType, range, ("title", Text("Lookbook is live.")))))),
            Seed(featureType, delivery, ("heading", Text("Free delivery")), ("text", Text("On orders over £50."))),
            Seed(featureType, colours, ("heading", Text("New colours")), ("text", Text("Three shades for the season.")))));
        _contentService.Save(home);
        var published = _contentService.Publish(home, ["*"]);
        _logger.LogInformation("Published Home version: {Result}", published.Result);
        await Task.Delay(1100, cancellationToken);

        home = _contentService.GetById(home.Id) ?? home;
        ApplyHome(home, "Summer launch", "A short introduction to the summer campaign, now with a longer note for editors.", true, BlockList(
            Seed(heroType, hero, ("heading", Text("Welcome back")), ("text", Text("Our summer range is here.")), ("highlights", BlockList(
                Seed(highlightType, range, ("title", Text("Lookbook is live.")))))),
            Seed(featureType, delivery, ("heading", Text("Free delivery")), ("text", Text("On orders over £50."))),
            Seed(featureType, colours, ("heading", Text("New colours")), ("text", Text("Four shades, including sand."))),
            Seed(quoteType, quote, ("quote", Text("The best season yet.")), ("attribution", Text("Editor")))));
        _contentService.Save(home);
        await Task.Delay(1100, cancellationToken);

        home = _contentService.GetById(home.Id) ?? home;
        ApplyHome(home, "Autumn launch", "The autumn campaign replaces the summer introduction.", false, BlockList(
            Seed(heroType, hero, ("heading", Text("Welcome back")), ("text", Text("The autumn range is in store now.")), ("highlights", BlockList(
                Seed(highlightType, range, ("title", Text("Lookbook updated for autumn."))),
                Seed(highlightType, workshops, ("title", Text("Store workshops")))))),
            Seed(featureType, colours, ("heading", Text("New colours")), ("text", Text("Four shades, including sand."))),
            Seed(quoteType, quote, ("quote", Text("The best season yet.")), ("attribution", Text("Content team"))),
            Seed(featureType, events, ("heading", Text("Store events")), ("text", Text("Saturday tastings through October.")))));
        _contentService.Save(home);
    }

    private async Task SeedAboutAsync(CancellationToken cancellationToken)
    {
        var about = _contentService.Create("About", Constants.System.Root, "aboutPage");
        about.SetValue("title", "About us");
        about.SetValue("summary", "We build Umbraco sites.");
        about.SetValue("body", "Founded in 2010.");
        _contentService.Save(about);
        _contentService.Publish(about, ["*"]);
        await Task.Delay(1100, cancellationToken);

        about = _contentService.GetById(about.Id) ?? about;
        about.SetValue("title", "About the team");
        about.SetValue("summary", "We build Umbraco sites for editors.");
        about.SetValue("body", "Founded in 2010. The content team sits in London.");
        _contentService.Save(about);
    }

    private static void ApplyHome(IContent content, string title, string intro, bool showBanner, JsonObject sections)
    {
        content.SetValue("title", title);
        content.SetValue("intro", intro);
        content.SetValue("showBanner", showBanner ? 1 : 0);
        content.SetValue("sections", sections.ToJsonString());
    }

    private ContentType CreateElement(string alias, string name, string icon)
        => new(_shortStringHelper, -1)
        {
            Alias = alias,
            Name = name,
            Icon = icon,
            IsElement = true,
        };

    private ContentType CreateDocument(string alias, string name, string icon)
        => new(_shortStringHelper, -1)
        {
            Alias = alias,
            Name = name,
            Icon = icon,
            AllowedAsRoot = true,
        };

    private void AddProperty(ContentType contentType, IDataType dataType, string alias, string name, int sortOrder)
    {
        var property = new PropertyType(_shortStringHelper, dataType, alias)
        {
            Name = name,
            SortOrder = sortOrder,
        };
        contentType.AddPropertyType(property, "Content", "content");
    }

    private IDataType CreateBlockList(string name, params Guid[] elementTypeKeys)
    {
        var editor = _propertyEditors[Constants.PropertyEditors.Aliases.BlockList];
        var configuration = new BlockListConfiguration
        {
            Blocks = elementTypeKeys
                .Select(key => new BlockListConfiguration.BlockConfiguration { ContentElementTypeKey = key })
                .ToArray(),
        };
        var dataType = new DataType(editor, _serializer)
        {
            Name = name,
            DatabaseType = ValueStorageType.Ntext,
            EditorUiAlias = "Umb.PropertyEditorUi.BlockList",
        };
        var configurationEditor = editor.GetConfigurationEditor()
            ?? throw new InvalidOperationException("Block List has no configuration editor.");
        dataType.ConfigurationData = configurationEditor.FromConfigurationObject(configuration, _serializer);
        _dataTypeService.Save(dataType);
        return dataType;
    }

    private IDataType PickDataType(string editorAlias, params string[] preferredNames)
    {
        var matches = _dataTypeService.GetAll().Where(dataType => dataType.EditorAlias == editorAlias).ToList();
        foreach (var name in preferredNames)
        {
            var match = matches.FirstOrDefault(dataType => dataType.Name?.Equals(name, StringComparison.OrdinalIgnoreCase) == true);
            if (match is not null)
            {
                return match;
            }
        }

        return matches.FirstOrDefault()
            ?? throw new InvalidOperationException($"No data type uses editor {editorAlias}. Found: {string.Join(", ", _dataTypeService.GetAll().Select(dataType => dataType.Name))}");
    }

    private readonly record struct SeedBlock(Guid ElementTypeKey, Guid Key, (string Alias, JsonNode Value)[] Fields);

    private static SeedBlock Seed(Guid elementTypeKey, Guid key, params (string Alias, JsonNode Value)[] fields)
        => new(elementTypeKey, key, fields);

    private static JsonValue Text(string value) => JsonValue.Create(value)!;

    private static JsonObject BlockList(params SeedBlock[] blocks)
    {
        var contentData = new JsonArray();
        var layout = new JsonArray();
        var expose = new JsonArray();
        foreach (var block in blocks)
        {
            var values = new JsonArray();
            foreach (var (alias, value) in block.Fields)
            {
                values.Add(new JsonObject
                {
                    ["alias"] = alias,
                    ["culture"] = JsonNull(),
                    ["segment"] = JsonNull(),
                    ["value"] = value,
                });
            }

            contentData.Add(new JsonObject
            {
                ["contentTypeKey"] = block.ElementTypeKey.ToString("D"),
                ["key"] = block.Key.ToString("D"),
                ["values"] = values,
            });
            layout.Add(new JsonObject
            {
                ["contentKey"] = block.Key.ToString("D"),
                ["settingsKey"] = JsonNull(),
            });
            expose.Add(new JsonObject
            {
                ["contentKey"] = block.Key.ToString("D"),
                ["culture"] = JsonNull(),
                ["segment"] = JsonNull(),
            });
        }

        return new JsonObject
        {
            ["contentData"] = contentData,
            ["settingsData"] = new JsonArray(),
            ["expose"] = expose,
            ["layout"] = new JsonObject
            {
                ["Umbraco.BlockList"] = layout,
            },
        };
    }

    private static JsonNode JsonNull() => JsonNode.Parse("null")!;
}
