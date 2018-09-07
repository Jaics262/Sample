namespace DbEngine.Query.Parser
{
    /* This class is used for storing name of field, aggregate function for 
    each aggregate function generate properties for this class, Also override toString method
   */
    public class AggregateFunction
    {
        string field, function;
        // Write logic for constructor
        public AggregateFunction(string field, string function)
        {
            this.field = field;
            this.function = function;
        }

        public override bool Equals(object obj)
        {
            AggregateFunction aggregateFunction = obj as AggregateFunction;
            return aggregateFunction.field == this.field && aggregateFunction.function == this.function;
        }

        public string GetField
        {
            get
            {
                return field;
            }
        }

        public string GetFunction
        {
            get
            {
                return function;
            }
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
    }
}