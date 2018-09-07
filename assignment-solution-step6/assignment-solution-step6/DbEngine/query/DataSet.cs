using System.Collections.Generic;

namespace DbEngine.Query
{
    //This class will be acting as the DataSet containing multiple rows in a property named Rows
    public class DataSet
    {
        List<Row> rowList;
        public DataSet()
        {
            rowList = new List<Row>();
        }

        public List<Row> Rows
        {
            get
            {
                return rowList;
            }
        }

        /*
	    This method will sort the dataSet based on the columnIndex
	    */
        public List<Row> SortData(string dataType, int columnIndex)
        {
           return null;
        }
    }
    
}
