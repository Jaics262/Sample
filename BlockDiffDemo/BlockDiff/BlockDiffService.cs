using System.Net;
using System.Text.Json;
using System.Text.RegularExpressions;
using Umbraco.Cms.Core;
using Umbraco.Cms.Core.Models;
using Umbraco.Cms.Core.Services;

namespace BlockDiffDemo.BlockDiff;

public class BlockDiffService
{
    private readonly IContentService _contentService;
    private readonly IContentTypeService _contentTypeService;

    public BlockDiffService(IContentService contentService, IContentTypeService contentTypeService)
    {
        _contentService = contentService;
        _contentTypeService = contentTypeService;
    }

    public VersionListResponse? GetVersions(Guid contentKey)
    {
        var content = _contentService.GetById(contentKey);
        if (content is null)
        {
            return null;
        }

        var versions = _contentService.GetVersions(content.Id).ToList();
        var items = versions.Select((version, index) => new VersionListItem
        {
            Id = version.VersionId,
            Date = version.UpdateDate,
            IsPublished = version.Published,
            IsCurrent = index == 0,
            Label = Label(version, index == 0),
        }).ToList();

        return new VersionListResponse
        {
            ContentName = content.Name ?? "Content",
            Versions = items,
        };
    }

    public DiffResponse? Compare(Guid contentKey, int fromVersionId, int toVersionId)
    {
        var current = _contentService.GetById(contentKey);
        if (current is null)
        {
            return null;
        }

        var from = _contentService.GetVersion(fromVersionId);
        var to = _contentService.GetVersion(toVersionId);
        if (from is null || to is null || from.Id != current.Id || to.Id != current.Id)
        {
            return null;
        }

        var contentType = _contentTypeService.Get(current.ContentTypeId);
        var elementTypes = new Dictionary<Guid, IContentType>();
        var summary = new List<string>();
        var properties = new List<DiffProperty>();

        var propertyTypes = contentType is null
            ? Enumerable.Empty<IPropertyType>()
            : contentType.CompositionPropertyTypes.OrderBy(property => property.SortOrder);
        foreach (var propertyType in propertyTypes)
        {
            var fromValue = from.GetValue(propertyType.Alias);
            var toValue = to.GetValue(propertyType.Alias);
            if (propertyType.PropertyEditorAlias == Constants.PropertyEditors.Aliases.BlockList)
            {
                var blocks = DiffBlocks(ReadBlocks(fromValue, elementTypes), ReadBlocks(toValue, elementTypes), summary, propertyType.Name);
                properties.Add(new DiffProperty
                {
                    Name = propertyType.Name,
                    Alias = propertyType.Alias,
                    Kind = "blocks",
                    Status = blocks.Any(block => block.Status != "unchanged") ? "changed" : "unchanged",
                    Blocks = blocks,
                });
                continue;
            }

            var fromText = Display(fromValue, propertyType.PropertyEditorAlias);
            var toText = Display(toValue, propertyType.PropertyEditorAlias);
            var status = StatusOf(fromText, toText);
            if (status != "unchanged")
            {
                summary.Add($"{propertyType.Name} {status}");
            }

            properties.Add(new DiffProperty
            {
                Name = propertyType.Name,
                Alias = propertyType.Alias,
                Kind = "text",
                Status = status,
                From = fromText,
                To = toText,
            });
        }

        var versions = _contentService.GetVersions(current.Id).ToList();
        return new DiffResponse
        {
            ContentName = current.Name ?? "Content",
            FromLabel = Label(from, versions.Count > 0 && versions[0].VersionId == from.VersionId),
            ToLabel = Label(to, versions.Count > 0 && versions[0].VersionId == to.VersionId),
            Summary = summary,
            ChangeCount = summary.Count,
            Properties = properties,
        };
    }

    private List<DiffBlock> DiffBlocks(IReadOnlyList<ParsedBlock> fromBlocks, IReadOnlyList<ParsedBlock> toBlocks, List<string> summary, string? parentName)
    {
        var fromByKey = fromBlocks.ToDictionary(block => block.Key);
        var toKeys = toBlocks.Select(block => block.Key).ToHashSet();
        var result = new List<DiffBlock>();

        foreach (var toBlock in toBlocks)
        {
            if (!fromByKey.TryGetValue(toBlock.Key, out var fromBlock))
            {
                summary.Add(Describe(parentName, toBlock.Label, "added"));
                result.Add(ToDiffBlock(toBlock, "added", toBlock.Fields.Select(field => FieldAdded(field, summary, toBlock.Label)).ToList()));
                continue;
            }

            var fields = new List<DiffField>();
            var fieldNames = fromBlock.Fields.Select(field => field.Alias).Union(toBlock.Fields.Select(field => field.Alias));
            var changed = false;
            foreach (var alias in fieldNames)
            {
                var fromField = fromBlock.Fields.FirstOrDefault(field => field.Alias == alias);
                var toField = toBlock.Fields.FirstOrDefault(field => field.Alias == alias);
                var field = DiffFieldPair(fromField, toField, summary, toBlock.Label);
                if (field.Status != "unchanged")
                {
                    changed = true;
                }

                fields.Add(field);
            }

            result.Add(ToDiffBlock(toBlock, changed ? "changed" : "unchanged", fields));
        }

        foreach (var fromBlock in fromBlocks.Where(block => !toKeys.Contains(block.Key)))
        {
            summary.Add(Describe(parentName, fromBlock.Label, "removed"));
            result.Add(ToDiffBlock(fromBlock, "removed", fromBlock.Fields.Select(field => new DiffField
            {
                Name = field.Name,
                Status = "removed",
                From = field.Text,
            }).ToList()));
        }

        return result;
    }

    private DiffField DiffFieldPair(ParsedField? fromField, ParsedField? toField, List<string> summary, string blockLabel)
    {
        var name = toField?.Name ?? fromField?.Name ?? "Field";
        if (fromField?.Blocks is not null || toField?.Blocks is not null)
        {
            var nested = DiffBlocks(fromField?.Blocks ?? [], toField?.Blocks ?? [], summary, blockLabel);
            var status = nested.Any(block => block.Status != "unchanged") ? "changed" : "unchanged";
            return new DiffField { Name = name, Status = status, Blocks = nested };
        }

        var fromText = fromField?.Text ?? "";
        var toText = toField?.Text ?? "";
        var fieldStatus = StatusOf(fromText, toText);
        if (fieldStatus != "unchanged")
        {
            summary.Add($"{blockLabel}: {name} {fieldStatus}");
        }

        return new DiffField { Name = name, Status = fieldStatus, From = fromText, To = toText };
    }

    private static DiffField FieldAdded(ParsedField field, List<string> summary, string blockLabel)
    {
        if (field.Blocks is not null)
        {
            return new DiffField
            {
                Name = field.Name,
                Status = "added",
                Blocks = field.Blocks.Select(block => new DiffBlock
                {
                    Name = block.ElementName,
                    Label = block.Label,
                    Status = "added",
                    Fields = block.Fields.Select(nested => new DiffField { Name = nested.Name, Status = "added", To = nested.Text }).ToList(),
                }).ToList(),
            };
        }

        return new DiffField { Name = field.Name, Status = "added", To = field.Text };
    }

    private static DiffBlock ToDiffBlock(ParsedBlock block, string status, IReadOnlyList<DiffField> fields)
        => new()
        {
            Name = block.ElementName,
            Label = block.Label,
            Status = status,
            Fields = fields,
        };

    private List<ParsedBlock> ReadBlocks(object? raw, Dictionary<Guid, IContentType> elementTypes)
    {
        if (!TryGetBlockRoot(raw, out var root))
        {
            return [];
        }

        if (!root.TryGetProperty("layout", out var layout) ||
            !layout.TryGetProperty("Umbraco.BlockList", out var items) ||
            items.ValueKind != JsonValueKind.Array)
        {
            return [];
        }

        var contentByKey = new Dictionary<string, JsonElement>(StringComparer.OrdinalIgnoreCase);
        if (root.TryGetProperty("contentData", out var contentData) && contentData.ValueKind == JsonValueKind.Array)
        {
            foreach (var item in contentData.EnumerateArray())
            {
                if (item.TryGetProperty("key", out var key))
                {
                    contentByKey[key.GetString() ?? ""] = item;
                }
            }
        }

        var blocks = new List<ParsedBlock>();
        foreach (var item in items.EnumerateArray())
        {
            if (!item.TryGetProperty("contentKey", out var contentKeyProperty))
            {
                continue;
            }

            var keyText = contentKeyProperty.GetString() ?? "";
            if (!contentByKey.TryGetValue(keyText, out var data) || !Guid.TryParse(keyText, out var key))
            {
                continue;
            }

            var elementTypeKey = data.TryGetProperty("contentTypeKey", out var typeKey) && Guid.TryParse(typeKey.GetString(), out var parsedType)
                ? parsedType
                : Guid.Empty;
            var elementType = GetElementType(elementTypeKey, elementTypes);
            var fields = new List<ParsedField>();
            if (data.TryGetProperty("values", out var values) && values.ValueKind == JsonValueKind.Array)
            {
                foreach (var value in values.EnumerateArray())
                {
                    var alias = value.TryGetProperty("alias", out var aliasProperty) ? aliasProperty.GetString() ?? "" : "";
                    var property = elementType?.CompositionPropertyTypes.FirstOrDefault(candidate => candidate.Alias == alias);
                    var stored = value.TryGetProperty("value", out var storedValue) ? storedValue : default;
                    if (TryGetBlockRoot(stored, out _))
                    {
                        fields.Add(new ParsedField(alias, property?.Name ?? alias, null, ReadBlocks(stored, elementTypes)));
                        continue;
                    }

                    fields.Add(new ParsedField(alias, property?.Name ?? alias, ReadScalar(stored), null));
                }
            }

            var labelSource = fields.FirstOrDefault(field => !string.IsNullOrWhiteSpace(field.Text))?.Text;
            blocks.Add(new ParsedBlock(
                key,
                elementType?.Name ?? "Block",
                string.IsNullOrWhiteSpace(labelSource) ? elementType?.Name ?? "Block" : $"{elementType?.Name ?? "Block"} · {labelSource}",
                fields));
        }

        return blocks;
    }

    private IContentType? GetElementType(Guid key, Dictionary<Guid, IContentType> cache)
    {
        if (key == Guid.Empty)
        {
            return null;
        }

        if (cache.TryGetValue(key, out var cached))
        {
            return cached;
        }

        var contentType = _contentTypeService.Get(key);
        if (contentType is not null)
        {
            cache[key] = contentType;
        }

        return contentType;
    }

    private static bool TryGetBlockRoot(object? raw, out JsonElement root)
    {
        root = default;
        switch (raw)
        {
            case null:
                return false;
            case string text when string.IsNullOrWhiteSpace(text):
                return false;
            case string text:
                try
                {
                    using var document = JsonDocument.Parse(text);
                    if (!LooksLikeBlockList(document.RootElement))
                    {
                        return false;
                    }

                    root = document.RootElement.Clone();
                    return true;
                }
                catch (JsonException)
                {
                    return false;
                }
            case JsonElement element when LooksLikeBlockList(element):
                root = element.Clone();
                return true;
            default:
                return false;
        }
    }

    private static bool LooksLikeBlockList(JsonElement element)
        => element.ValueKind == JsonValueKind.Object &&
           element.TryGetProperty("layout", out var layout) &&
           layout.ValueKind == JsonValueKind.Object &&
           layout.TryGetProperty("Umbraco.BlockList", out _);

    private static string ReadScalar(JsonElement value) => value.ValueKind switch
    {
        JsonValueKind.String => value.GetString() ?? "",
        JsonValueKind.True => "Yes",
        JsonValueKind.False => "No",
        JsonValueKind.Number => value.ToString(),
        JsonValueKind.Null or JsonValueKind.Undefined => "",
        _ => value.ToString(),
    };

    private static string Display(object? value, string editorAlias)
    {
        if (value is null)
        {
            return "";
        }

        if (editorAlias == Constants.PropertyEditors.Aliases.Boolean)
        {
            return value switch
            {
                bool flag => flag ? "Yes" : "No",
                int number => number == 1 ? "Yes" : "No",
                string text when text is "1" or "true" => "Yes",
                string => "No",
                _ => value.ToString() ?? "",
            };
        }

        if (editorAlias == Constants.PropertyEditors.Aliases.RichText)
        {
            return RichTextToPlain(value.ToString() ?? "");
        }

        return value.ToString()?.Trim() ?? "";
    }

    private static string RichTextToPlain(string raw)
    {
        var markup = raw;
        try
        {
            using var document = JsonDocument.Parse(raw);
            if (document.RootElement.ValueKind == JsonValueKind.Object &&
                document.RootElement.TryGetProperty("markup", out var markupElement) &&
                markupElement.ValueKind == JsonValueKind.String)
            {
                markup = markupElement.GetString() ?? "";
            }
        }
        catch (JsonException)
        {
        }

        var withLinks = Regex.Replace(
            markup,
            "<a\\b[^>]*\\bhref\\s*=\\s*([\"'])(.*?)\\1[^>]*>(.*?)</a>",
            match =>
            {
                var href = WebUtility.HtmlDecode(match.Groups[2].Value).Trim();
                var text = WebUtility.HtmlDecode(Regex.Replace(match.Groups[3].Value, "<[^>]+>", "")).Trim();
                if (href.Length == 0)
                {
                    return text;
                }

                var target = href.Contains("localLink:", StringComparison.OrdinalIgnoreCase) ? "content link" : href;
                return text.Length == 0 ? $"({target})" : $"{text} ({target})";
            },
            RegexOptions.IgnoreCase | RegexOptions.Singleline);
        var withBreaks = Regex.Replace(withLinks, "</p>|<br\\s*/?>|</li>|</h[1-6]>", "\n", RegexOptions.IgnoreCase);
        var stripped = Regex.Replace(withBreaks, "<[^>]+>", "");
        return WebUtility.HtmlDecode(stripped).Trim();
    }

    private static string StatusOf(string from, string to)
    {
        if (string.Equals(from, to, StringComparison.Ordinal))
        {
            return "unchanged";
        }

        if (from.Length == 0)
        {
            return "added";
        }

        if (to.Length == 0)
        {
            return "removed";
        }

        return "changed";
    }

    private static string Label(IContent version, bool isCurrent)
    {
        var state = version.Published ? "Published" : isCurrent ? "Current draft" : "Saved";
        return $"{version.UpdateDate.ToLocalTime():dd MMM yyyy, HH:mm:ss} · {state}";
    }

    private static string Describe(string? parent, string label, string status)
        => string.IsNullOrWhiteSpace(parent) ? $"{label} {status}" : $"{parent}: {label} {status}";

    private sealed record ParsedBlock(Guid Key, string ElementName, string Label, IReadOnlyList<ParsedField> Fields);

    private sealed record ParsedField(string Alias, string Name, string? Text, IReadOnlyList<ParsedBlock>? Blocks);
}
