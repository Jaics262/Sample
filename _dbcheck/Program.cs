using Microsoft.Data.Sqlite;
var db = Path.GetFullPath(args[0]);
using (var w = new SqliteConnection($"Data Source={db}")) { w.Open(); using var c=w.CreateCommand(); c.CommandText="PRAGMA wal_checkpoint(FULL);"; c.ExecuteNonQuery(); }
using var conn = new SqliteConnection($"Data Source={db};Mode=ReadOnly");
conn.Open();
void Q(string sql) {
  Console.WriteLine(sql);
  using var cmd = conn.CreateCommand(); cmd.CommandText = sql;
  using var r = cmd.ExecuteReader();
  var n=0;
  while (r.Read()) {
    n++;
    var parts = new string[r.FieldCount];
    for (var i=0;i<r.FieldCount;i++){ var v=r.IsDBNull(i)?"NULL":Convert.ToString(r.GetValue(i))??""; if(v.Length>160)v=v[..160]+"..."; parts[i]=v; }
    Console.WriteLine(string.Join(" | ", parts));
  }
  Console.WriteLine("rows="+n);
  Console.WriteLine();
}
Q("PRAGMA table_info(umbracoNode);");
Q("SELECT id, uniqueId, text, path FROM umbracoNode WHERE text='Home' OR lower(uniqueId) LIKE '12be2657%';");
Q("SELECT nodeId, published, edited FROM umbracoDocument;");
Q(@"SELECT v.id, v.nodeId, v.[current], dv.published, v.versionDate
FROM umbracoContentVersion v LEFT JOIN umbracoDocumentVersion dv ON dv.id=v.id
ORDER BY v.nodeId, v.id;");
Q(@"SELECT v.id, pt.Alias, COALESCE(pd.varcharValue,'') AS varcharValue, substr(COALESCE(pd.textValue,''),1,60) AS textValue
FROM umbracoContentVersion v
JOIN umbracoPropertyData pd ON pd.versionId=v.id
JOIN cmsPropertyType pt ON pt.id=pd.propertyTypeId
WHERE pt.Alias='title' AND v.nodeId IN (SELECT id FROM umbracoNode WHERE text='Home')
ORDER BY v.id;");
