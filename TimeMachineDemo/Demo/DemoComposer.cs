using Umbraco.Cms.Core.Composing;
using Umbraco.Cms.Core.Notifications;

namespace TimeMachineDemo.Demo;

public class DemoComposer : IComposer
{
    public void Compose(IUmbracoBuilder builder)
    {
        builder.AddNotificationAsyncHandler<UmbracoApplicationStartedNotification, DemoContentSeeder>();
        builder.AddNotificationAsyncHandler<UmbracoApplicationStartedNotification, DemoTemplateInstaller>();
        builder.AddNotificationHandler<RoutingRequestNotification, DemoTemplateRouter>();
    }
}
