using Umbraco.Cms.Core.Composing;

namespace BlockDiffDemo.BlockDiff;

public class BlockDiffComposer : IComposer
{
    public void Compose(IUmbracoBuilder builder)
        => builder.Services.AddScoped<BlockDiffService>();
}
