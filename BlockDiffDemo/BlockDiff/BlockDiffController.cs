using Microsoft.AspNetCore.Mvc;
using Umbraco.Cms.Api.Management.Controllers;
using Umbraco.Cms.Api.Management.Routing;
using Umbraco.Cms.Core.Security;

namespace BlockDiffDemo.BlockDiff;

[VersionedApiBackOfficeRoute("block-diff")]
[ApiExplorerSettings(GroupName = "Block diff")]
public class BlockDiffController : ManagementApiControllerBase
{
    private readonly BlockDiffService _blockDiffService;
    private readonly IBackOfficeSecurityAccessor _backOfficeSecurityAccessor;

    public BlockDiffController(
        BlockDiffService blockDiffService,
        IBackOfficeSecurityAccessor backOfficeSecurityAccessor)
    {
        _blockDiffService = blockDiffService;
        _backOfficeSecurityAccessor = backOfficeSecurityAccessor;
    }

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

    [HttpPost("content/{contentId:guid}/rollback-remark")]
    public IActionResult RecordRollbackRemark(
        Guid contentId,
        [FromBody] RollbackRemarkRequest request)
    {
        if (_backOfficeSecurityAccessor.BackOfficeSecurity?.CurrentUser is null)
        {
            return Unauthorized();
        }

        var saved = _blockDiffService.RecordRollbackRemark(contentId, request.Remark);
        return saved ? Ok() : NotFound();
    }
}
