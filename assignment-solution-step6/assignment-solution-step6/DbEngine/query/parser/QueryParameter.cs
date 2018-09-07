using System;
using System.Collections.Generic;

namespace DbEngine.Query.Parser
{
    /* 
  This class will contain the elements of the parsed Query String such as conditions, logical operators,aggregate functions, file name, fields group by fields, order by   fields, Query Type
  */
    public class QueryParameter {
        public string QueryString { get; set; }
        public List<Restriction> Restrictions { get; set; }
        public List<String> LogicalOperators { get; set; }
        public List<AggregateFunction> AggregateFunctions { get; set; }
        public string File { get; set; }
        public string BaseQuery { get; set; }
        public List<String> Fields { get; set; }
        public List<String> GroupByFields { get; set; }
        public List<String> OrderByFields { get; set; }
        // Query type may be simple, group by, order by, aggregate
        public string QueryType { get; set; } = "SIMPLE_QUERY";
    }
}