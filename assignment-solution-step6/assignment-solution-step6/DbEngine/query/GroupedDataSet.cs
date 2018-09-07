using System.Collections.Generic;

namespace DbEngine.Query
{
    /*
 * Processing queries with group by clause will result in GroupedDataSet which will contain multiple dataSets, 
 * each of them indexed with the key column. Hence, the structure has been taken as a subtype of DataSet
 */

    public class GroupDataSet : DataSet
    {
        Dictionary<string, object> groupedDataSet;

        List<Row> rowList;
        public GroupDataSet()
        {
            groupedDataSet = new Dictionary<string, object>();
        }

        public Dictionary<string, object> GroupedDataSet
        {
            get
            {
                return groupedDataSet;
            }
        }

    }
}
