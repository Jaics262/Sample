namespace BlockDiffDemo.BlockDiff;

public sealed class VersionListResponse
{
    public string ContentName { get; set; } = "";

    public IReadOnlyList<VersionListItem> Versions { get; set; } = [];
}

public sealed class VersionListItem
{
    public int Id { get; set; }

    public DateTime Date { get; set; }

    public bool IsPublished { get; set; }

    public bool IsCurrent { get; set; }

    public string Label { get; set; } = "";

    public string PreviewUrl { get; set; } = "";

    public string? Remark { get; set; }
}

public sealed class RollbackRemarkRequest
{
    public string? Remark { get; set; }
}

public sealed class DiffResponse
{
    public string ContentName { get; set; } = "";

    public string FromLabel { get; set; } = "";

    public string ToLabel { get; set; } = "";

    public IReadOnlyList<DiffSummaryItem> Summary { get; set; } = [];

    public int ChangeCount { get; set; }

    public IReadOnlyList<DiffProperty> Properties { get; set; } = [];
}

public sealed class DiffSummaryItem
{
    public string Text { get; set; } = "";

    public string Target { get; set; } = "";
}

public sealed class DiffProperty
{
    public string Name { get; set; } = "";

    public string Alias { get; set; } = "";

    public string Anchor { get; set; } = "";

    public string Kind { get; set; } = "text";

    public string Status { get; set; } = "unchanged";

    public string? From { get; set; }

    public string? To { get; set; }

    public IReadOnlyList<DiffBlock> Blocks { get; set; } = [];
}

public sealed class DiffBlock
{
    public string Name { get; set; } = "";

    public string Label { get; set; } = "";

    public string Anchor { get; set; } = "";

    public string Status { get; set; } = "unchanged";

    public IReadOnlyList<DiffField> Fields { get; set; } = [];
}

public sealed class DiffField
{
    public string Name { get; set; } = "";

    public string Anchor { get; set; } = "";

    public string Status { get; set; } = "unchanged";

    public string? From { get; set; }

    public string? To { get; set; }

    public IReadOnlyList<DiffBlock> Blocks { get; set; } = [];
}
