
WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

builder.CreateUmbracoBuilder()
    .AddBackOffice()
    .AddWebsite()
    .AddComposers()
    .Build();

WebApplication app = builder.Build();


await app.BootUmbracoAsync();

app.Use(async (context, next) =>
{
    if (!IsBackofficePage(context.Request.Path))
    {
        await next();
        return;
    }

    var originalBody = context.Response.Body;
    using var buffer = new MemoryStream();
    context.Response.Body = buffer;
    try
    {
        await next();
        buffer.Position = 0;
        var contentType = context.Response.ContentType ?? "";
        if (!contentType.Contains("text/html", StringComparison.OrdinalIgnoreCase))
        {
            context.Response.Body = originalBody;
            await buffer.CopyToAsync(originalBody);
            return;
        }

        var html = await new StreamReader(buffer).ReadToEndAsync();
        const string script = "<script src=\"/App_Plugins/BlockDiff/login-fill.js\"></script>";
        if (!html.Contains(script, StringComparison.Ordinal) &&
            html.Contains("</body>", StringComparison.OrdinalIgnoreCase))
        {
            html = html.Replace("</body>", script + "</body>", StringComparison.OrdinalIgnoreCase);
        }

        var bytes = System.Text.Encoding.UTF8.GetBytes(html);
        context.Response.ContentLength = bytes.Length;
        context.Response.Body = originalBody;
        await context.Response.Body.WriteAsync(bytes);
    }
    finally
    {
        context.Response.Body = originalBody;
    }
});

app.UseUmbraco()
    .WithMiddleware(u =>
    {
        u.UseBackOffice();
        u.UseWebsite();
    })
    .WithEndpoints(u =>
    {
        u.UseBackOfficeEndpoints();
        u.UseWebsiteEndpoints();
    });

await app.RunAsync();

static bool IsBackofficePage(PathString path)
{
    var value = path.Value ?? "";
    if (!value.StartsWith("/umbraco", StringComparison.OrdinalIgnoreCase))
    {
        return false;
    }

    if (value.Contains("/management/", StringComparison.OrdinalIgnoreCase) ||
        value.Contains("/api/", StringComparison.OrdinalIgnoreCase) ||
        value.Contains("/backoffice/", StringComparison.OrdinalIgnoreCase))
    {
        return false;
    }

    return Path.GetExtension(value).Length == 0;
}
