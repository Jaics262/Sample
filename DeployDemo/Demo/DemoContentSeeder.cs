using System.Text.Json.Nodes;
using Umbraco.Cms.Core;
using Umbraco.Cms.Core.Events;
using Umbraco.Cms.Core.Models;
using Umbraco.Cms.Core.Notifications;
using Umbraco.Cms.Core.PropertyEditors;
using Umbraco.Cms.Core.Serialization;
using Umbraco.Cms.Core.Services;
using Umbraco.Cms.Core.Strings;

namespace DeployDemo.Demo;

public class DemoContentSeeder : INotificationAsyncHandler<UmbracoApplicationStartedNotification>
{
    private static readonly Guid Shipping = Guid.Parse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb1");
    private static readonly Guid HowLong = Guid.Parse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb2");
    private static readonly Guid Abroad = Guid.Parse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb3");
    private static readonly Guid Returns = Guid.Parse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb4");
    private static readonly Guid HowToReturn = Guid.Parse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb5");
    private static readonly Guid Postage = Guid.Parse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb6");
    private static readonly Guid Tracking = Guid.Parse("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbb7");

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
        var notesMissing = _contentTypeService.Get("notePage") is null;
        var faqMissing = _contentTypeService.Get("faqPage") is null;
        if (!notesMissing && !faqMissing)
        {
            return;
        }

        try
        {
            if (notesMissing)
            {
                await SeedNotesAsync(cancellationToken);
            }

            if (faqMissing)
            {
                await SeedFaqAsync(cancellationToken);
            }
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "Demo content was not seeded.");
        }
    }

    private async Task SeedNotesAsync(CancellationToken cancellationToken)
    {
        var text = PickDataType(Constants.PropertyEditors.Aliases.TextBox, "Textstring", "Text Box");
        var area = PickDataType(Constants.PropertyEditors.Aliases.TextArea, "Textarea", "Text Area");

        var note = new ContentType(_shortStringHelper, -1)
        {
            Alias = "notePage",
            Name = "Note",
            Icon = "icon-notepad",
            AllowedAsRoot = true,
        };
        AddProperty(note, text, "title", "Title", 0);
        AddProperty(note, area, "body", "Body", 1);
        _contentTypeService.Save(note);

        var welcome = _contentService.Create("Welcome", Constants.System.Root, "notePage");
        welcome.SetValue("title", "Welcome");
        welcome.SetValue("body", "First draft of the Deploy test page.");
        _contentService.Save(welcome);
        var published = _contentService.Publish(welcome, ["*"]);
        _logger.LogInformation("Published Welcome: {Result}", published.Result);

        await Pause(cancellationToken);

        welcome = _contentService.GetById(welcome.Id) ?? welcome;
        welcome.SetValue("title", "Welcome to Deploy");
        welcome.SetValue("body", "Updated draft so the Deploy demo has a second change.");
        _contentService.Save(welcome);

        var office = _contentService.Create("Office hours", Constants.System.Root, "notePage");
        office.SetValue("title", "Office hours");
        office.SetValue("body", "Monday to Friday, 9am to 5pm.");
        _contentService.Save(office);
        _contentService.Publish(office, ["*"]);
        _logger.LogInformation("Seeded Welcome and Office hours.");
    }

    private async Task SeedFaqAsync(CancellationToken cancellationToken)
    {
        var text = PickDataType(Constants.PropertyEditors.Aliases.TextBox, "Textstring", "Text Box");
        var area = PickDataType(Constants.PropertyEditors.Aliases.TextArea, "Textarea", "Text Area");

        var item = CreateElement("accordionItem", "Accordion item", "icon-list");
        AddProperty(item, text, "heading", "Heading", 0);
        AddProperty(item, area, "text", "Text", 1);
        _contentTypeService.Save(item);

        var items = CreateBlockList("Accordion items", item.Key);
        var accordion = CreateElement("accordionBlock", "Accordion", "icon-indent");
        AddProperty(accordion, text, "title", "Title", 0);
        AddProperty(accordion, items, "items", "Items", 1);
        _contentTypeService.Save(accordion);

        var accordions = CreateBlockList("Accordions", accordion.Key);
        var faq = new ContentType(_shortStringHelper, -1)
        {
            Alias = "faqPage",
            Name = "FAQ page",
            Icon = "icon-help-alt",
            AllowedAsRoot = true,
        };
        AddProperty(faq, text, "title", "Title", 0);
        AddProperty(faq, area, "intro", "Introduction", 1);
        AddProperty(faq, accordions, "accordions", "Accordions", 2);
        _contentTypeService.Save(faq);

        var page = _contentService.Create("Delivery questions", Constants.System.Root, "faqPage");
        ApplyFaq(page, "Delivery questions", "Common questions about orders.", Accordions(
            Accordion(accordion.Key, Shipping, "Shipping",
                Item(item.Key, HowLong, "How long does delivery take?", "3 to 5 days."),
                Item(item.Key, Abroad, "Do you ship abroad?", "Yes, to the UK and EU."))));
        _contentService.Save(page);
        var published = _contentService.Publish(page, ["*"]);
        _logger.LogInformation("Published Delivery questions: {Result}", published.Result);
        await Pause(cancellationToken);

        page = _contentService.GetById(page.Id) ?? page;
        ApplyFaq(page, "Delivery questions", "Common questions about orders.", Accordions(
            Accordion(accordion.Key, Shipping, "Shipping",
                Item(item.Key, HowLong, "How long does delivery take?", "3 to 5 days."),
                Item(item.Key, Abroad, "Do you ship abroad?", "Yes, to the UK and EU.")),
            Accordion(accordion.Key, Returns, "Returns",
                Item(item.Key, HowToReturn, "How do I return an item?", "Use the form within 30 days."),
                Item(item.Key, Postage, "Who pays the return postage?", "We email a prepaid label."))));
        _contentService.Save(page);
        await Pause(cancellationToken);

        page = _contentService.GetById(page.Id) ?? page;
        ApplyFaq(page, "Delivery questions", "Common questions about orders.", Accordions(
            Accordion(accordion.Key, Shipping, "Shipping",
                Item(item.Key, HowLong, "How long does delivery take?", "2 to 4 days for UK orders."),
                Item(item.Key, Abroad, "Do you ship abroad?", "Yes, to the UK and EU."),
                Item(item.Key, Tracking, "Where is my tracking link?", "It arrives by email once the parcel leaves the warehouse.")),
            Accordion(accordion.Key, Returns, "Returns",
                Item(item.Key, HowToReturn, "How do I return an item?", "Use the form within 30 days."),
                Item(item.Key, Postage, "Who pays the return postage?", "We email a prepaid label."))));
        _contentService.Save(page);
        await Pause(cancellationToken);

        page = _contentService.GetById(page.Id) ?? page;
        ApplyFaq(page, "Delivery questions", "Common questions about orders.", Accordions(
            Accordion(accordion.Key, Shipping, "Shipping and tracking",
                Item(item.Key, HowLong, "How long does delivery take?", "2 to 4 days for UK orders."),
                Item(item.Key, Tracking, "Where is my tracking link?", "It arrives by email once the parcel leaves the warehouse.")),
            Accordion(accordion.Key, Returns, "Returns",
                Item(item.Key, HowToReturn, "How do I return an item?", "Start a return from your account within 14 days."),
                Item(item.Key, Postage, "Who pays the return postage?", "We email a prepaid label."))));
        _contentService.Save(page);
        await Pause(cancellationToken);

        page = _contentService.GetById(page.Id) ?? page;
        ApplyFaq(page, "Delivery and returns", "Updated answers for the autumn range.", Accordions(
            Accordion(accordion.Key, Shipping, "Shipping and tracking",
                Item(item.Key, HowLong, "How long does delivery take?", "2 to 4 days for UK orders."),
                Item(item.Key, Tracking, "Where is my tracking link?", "It arrives by email once the parcel leaves the warehouse.")),
            Accordion(accordion.Key, Returns, "Returns",
                Item(item.Key, HowToReturn, "How do I return an item?", "Start a return from your account within 14 days."),
                Item(item.Key, Postage, "Who pays the return postage?", "Returns are free. We email a prepaid label."))));
        _contentService.Save(page);
        published = _contentService.Publish(page, ["*"]);
        _logger.LogInformation("Published accordion revisions: {Result}", published.Result);
    }

    private static async Task Pause(CancellationToken cancellationToken)
        => await Task.Delay(1500, cancellationToken);

    private static void ApplyFaq(IContent content, string title, string intro, JsonObject accordions)
    {
        content.SetValue("title", title);
        content.SetValue("intro", intro);
        content.SetValue("accordions", accordions.ToJsonString());
    }

    private ContentType CreateElement(string alias, string name, string icon)
        => new(_shortStringHelper, -1)
        {
            Alias = alias,
            Name = name,
            Icon = icon,
            IsElement = true,
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
            ?? throw new InvalidOperationException($"No data type uses editor {editorAlias}.");
    }

    private readonly record struct SeedBlock(Guid ElementTypeKey, Guid Key, (string Alias, JsonNode Value)[] Fields);

    private static SeedBlock Item(Guid elementTypeKey, Guid key, string heading, string text)
        => new(elementTypeKey, key, [("heading", Text(heading)), ("text", Text(text))]);

    private static SeedBlock Accordion(Guid elementTypeKey, Guid key, string title, params SeedBlock[] items)
        => new(elementTypeKey, key, [("title", Text(title)), ("items", BlockList(items))]);

    private static JsonObject Accordions(params SeedBlock[] blocks) => BlockList(blocks);

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
