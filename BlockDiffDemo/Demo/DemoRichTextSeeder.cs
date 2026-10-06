using System.Text.Json;
using Umbraco.Cms.Core;
using Umbraco.Cms.Core.Events;
using Umbraco.Cms.Core.Models;
using Umbraco.Cms.Core.Notifications;
using Umbraco.Cms.Core.Scoping;
using Umbraco.Cms.Core.Strings;
using Umbraco.Cms.Core.Services;

namespace BlockDiffDemo.Demo;

public class DemoRichTextSeeder : INotificationAsyncHandler<UmbracoApplicationStartedNotification>
{
    private const string PropertyAlias = "story";
    private const string EditedMarker = "Saturday tastings through October";

    private const string OriginalMarkup =
        "<p>The summer campaign opens with a lookbook and free delivery on orders over £50.</p><p>Three shades are available for the season.</p>";

    private const string EditedMarkup =
        "<p>The autumn campaign opens with a lookbook and Saturday tastings through October.</p><p>Four shades are available, including sand.</p><ul><li>Store workshops are new this season.</li></ul>";

    private readonly IContentService _contentService;
    private readonly IContentTypeService _contentTypeService;
    private readonly IDataTypeService _dataTypeService;
    private readonly ICoreScopeProvider _scopeProvider;
    private readonly IShortStringHelper _shortStringHelper;
    private readonly ILogger<DemoRichTextSeeder> _logger;

    public DemoRichTextSeeder(
        IContentService contentService,
        IContentTypeService contentTypeService,
        IDataTypeService dataTypeService,
        ICoreScopeProvider scopeProvider,
        IShortStringHelper shortStringHelper,
        ILogger<DemoRichTextSeeder> logger)
    {
        _contentService = contentService;
        _contentTypeService = contentTypeService;
        _dataTypeService = dataTypeService;
        _scopeProvider = scopeProvider;
        _shortStringHelper = shortStringHelper;
        _logger = logger;
    }

    public Task HandleAsync(UmbracoApplicationStartedNotification notification, CancellationToken cancellationToken)
    {
        try
        {
            var contentType = _contentTypeService.Get("landingPage");
            if (contentType is null)
            {
                return Task.CompletedTask;
            }

            if (contentType.PropertyTypes.All(property => property.Alias != PropertyAlias))
            {
                var dataType = _dataTypeService.GetAll()
                    .FirstOrDefault(item => item.EditorAlias == Constants.PropertyEditors.Aliases.RichText)
                    ?? throw new InvalidOperationException("No Rich Text data type is installed.");
                var property = new PropertyType(_shortStringHelper, dataType, PropertyAlias)
                {
                    Name = "Story",
                    SortOrder = 4,
                };
                contentType.AddPropertyType(property, "Content", "content");
                _contentTypeService.Save(contentType);
                contentType = _contentTypeService.Get("landingPage") ?? contentType;
            }

            var home = _contentService.GetRootContent().FirstOrDefault(item => item.ContentType.Alias == "landingPage");
            if (home is null)
            {
                return Task.CompletedTask;
            }

            home = _contentService.GetById(home.Id) ?? home;
            if (home.GetValue(PropertyAlias) is string current && current.Contains(EditedMarker, StringComparison.Ordinal))
            {
                return Task.CompletedTask;
            }

            var propertyType = contentType.PropertyTypes.First(property => property.Alias == PropertyAlias);
            var original = RichText(OriginalMarkup);
            using (var coreScope = _scopeProvider.CreateCoreScope())
            {
                var scope = (Umbraco.Cms.Infrastructure.Scoping.IScope)coreScope;
                foreach (var version in _contentService.GetVersions(home.Id).Skip(1))
                {
                    var existing = scope.Database.ExecuteScalar<int>(
                        "SELECT COUNT(*) FROM umbracoPropertyData WHERE versionId = @0 AND propertyTypeId = @1",
                        version.VersionId,
                        propertyType.Id);
                    if (existing > 0)
                    {
                        continue;
                    }

                    scope.Database.Execute(
                        "INSERT INTO umbracoPropertyData (versionId, propertyTypeId, languageId, segment, textValue) VALUES (@0, @1, @2, @3, @4)",
                        version.VersionId,
                        propertyType.Id,
                        null,
                        null,
                        original);
                }

                coreScope.Complete();
            }

            home.SetValue(PropertyAlias, RichText(EditedMarkup));
            _contentService.Save(home);
            _logger.LogInformation("Added the Story rich text field, with a different value on the latest Home version.");
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "The Story rich text field was not added.");
        }

        return Task.CompletedTask;
    }

    private static string RichText(string markup)
        => JsonSerializer.Serialize(new Dictionary<string, object?>
        {
            ["markup"] = markup,
            ["blocks"] = null,
        });
}
