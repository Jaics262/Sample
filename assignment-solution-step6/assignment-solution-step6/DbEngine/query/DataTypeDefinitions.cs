namespace DbEngine.Query
{
    /*
       This class should contain a member variable which is a String array, to hold the data type for all columns for all data types
      */
    public class DataTypeDefinitions
    {
        public string[] DataTypes { get; set; }

        public DataTypeDefinitions()
        {

        }

        public override string ToString()
        {
            return base.ToString();
        }
    }
}