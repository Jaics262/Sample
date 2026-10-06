using Umbraco.Cms.Core.Composing;
using Umbraco.Cms.Core.Notifications;

namespace BlockDiffDemo.Demo;

public class DemoComposer : IComposer
{
    public void Compose(IUmbracoBuilder builder)
    {
        builder.AddNotificationAsyncHandler<UmbracoApplicationStartedNotification, DemoContentSeeder>();
        builder.AddNotificationAsyncHandler<UmbracoApplicationStartedNotification, DemoTemplateInstaller>();
        builder.AddNotificationAsyncHandler<UmbracoApplicationStartedNotification, DemoRichTextSeeder>();
        builder.AddNotificationHandler<RoutingRequestNotification, DemoTemplateRouter>();
    }
}
