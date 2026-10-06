using Microsoft.AspNetCore.Mvc;
using Umbraco.Cms.Api.Management.Controllers;
using Umbraco.Cms.Api.Management.Routing;

namespace BlockDiffDemo.BlockDiff;

[VersionedApiBackOfficeRoute("block-diff")]
[ApiExplorerSettings(GroupName = "Block diff")]
public class BlockDiffController : ManagementApiControllerBase
{
    private readonly BlockDiffService _blockDiffService;

    public BlockDiffController(BlockDiffService blockDiffService)
        => _blockDiffService = blockDiffService;

    [HttpGet("content/{contentId:guid}/versions")]
    public IActionResult GetVersions(Guid contentId)
    {
        var versions = _blockDiffService.GetVersions(contentId);
        return versions is null ? NotFound() : Ok(versions);
    }

    [HttpGet("content/{contentId:guid}/compare")]
    public IActionResult Compare(Guid contentId, [FromQuery] int from, [FromQuery] int to)
    {
        var diff = _blockDiffService.Compare(contentId, from, to);
        return diff is null ? NotFound() : Ok(diff);
    }
}
