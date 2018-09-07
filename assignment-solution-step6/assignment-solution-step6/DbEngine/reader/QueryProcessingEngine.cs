using DbEngine.Query;
using DbEngine.Query.Parser;

namespace DbEngine.Reader
{
    public abstract class QueryProcessingEngine
    {
        public abstract Header GetHeader();
        public abstract DataSet GetDataRow(QueryParameter queryParameter);
        public abstract DataTypeDefinitions GetColumnType();
    }
}