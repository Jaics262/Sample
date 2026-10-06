using Umbraco.Cms.Core;
using Umbraco.Cms.Core.Events;
using Umbraco.Cms.Core.Models;
using Umbraco.Cms.Core.Notifications;
using Umbraco.Cms.Core.Routing;
using Umbraco.Cms.Core.Services;

namespace BlockDiffDemo.Demo;

public class DemoTemplateInstaller : INotificationAsyncHandler<UmbracoApplicationStartedNotification>
{
    private static readonly string[] Aliases = ["landingPage", "aboutPage"];

    private readonly ITemplateService _templateService;
    private readonly IContentTypeService _contentTypeService;
    private readonly IWebHostEnvironment _environment;
    private readonly ILogger<DemoTemplateInstaller> _logger;

    public DemoTemplateInstaller(
        ITemplateService templateService,
        IContentTypeService contentTypeService,
        IWebHostEnvironment environment,
        ILogger<DemoTemplateInstaller> logger)
    {
        _templateService = templateService;
        _contentTypeService = contentTypeService;
        _environment = environment;
        _logger = logger;
    }

    public async Task HandleAsync(UmbracoApplicationStartedNotification notification, CancellationToken cancellationToken)
    {
        try
        {
            foreach (var alias in Aliases)
            {
                await EnsureTemplateAsync(alias);
            }
        }
        catch (Exception exception)
        {
            _logger.LogError(exception, "Page templates were not attached. The visual rollback preview needs them.");
        }
    }

    private async Task EnsureTemplateAsync(string alias)
    {
        var viewPath = Path.Combine(_environment.ContentRootPath, "Views", $"{alias}.cshtml");
        var markup = await System.IO.File.ReadAllTextAsync(viewPath);
        var contentType = _contentTypeService.Get(alias);
        var template = await _templateService.GetAsync(alias);
        if (template is null)
        {
            var attempt = await _templateService.CreateForContentTypeAsync(
                contentType?.Name ?? alias,
                alias,
                alias,
                Constants.Security.SuperUserKey);
            template = attempt.Result;
            if (template is null)
            {
                _logger.LogWarning("Template {Alias} was not created ({Status}).", alias, attempt.Status);
                return;
            }
        }

        await System.IO.File.WriteAllTextAsync(viewPath, markup);

        if (contentType is null || template is null || contentType.DefaultTemplate?.Id == template.Id)
        {
            return;
        }

        contentType.AllowedTemplates = [template];
        contentType.SetDefaultTemplate(template);
        _contentTypeService.Save(contentType);
    }
}

public class DemoTemplateRouter : INotificationHandler<RoutingRequestNotification>
{
    private readonly ITemplateService _templateService;

    public DemoTemplateRouter(ITemplateService templateService)
        => _templateService = templateService;

    public void Handle(RoutingRequestNotification notification)
    {
        var request = notification.RequestBuilder;
        if (request.Template is not null)
        {
            return;
        }

        var alias = request.PublishedContent?.ContentType.Alias;
        if (string.IsNullOrEmpty(alias))
        {
            return;
        }

        var template = _templateService.GetAsync(alias).GetAwaiter().GetResult();
        if (template is not null)
        {
            request.SetTemplate(template);
        }
    }
}
