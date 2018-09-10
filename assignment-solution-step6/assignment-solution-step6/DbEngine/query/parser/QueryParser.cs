
using System;
using System.Linq;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Text;
using DbEngine.query.parser;

namespace DbEngine.Query.Parser
{

    public class QueryParser
    {
        private QueryParameter queryParameter;
        public QueryParser()
        {
            queryParameter = new QueryParameter();
        }


        public QueryParameter ParseQuery(string queryString, int priority)
        {

            switch (priority)
            {
                case 1:
                    queryParameter.AggregateFunctions = GetAggregateFunctions(queryString);
                    break;
                case 2:
                    queryParameter.Fields = GetFields(queryString);
                    // queryParameter.QueryType = "SIMPLE_QUERY_WITH_FIELDS";
                    break;
                case 3:
                    queryParameter.File = GetFileName(queryString);
                    //queryParameter.QueryType = "SIMPLE_QUERY";
                    break;
                case 4:
                    queryParameter.Fields = GetFields(queryString);
                    queryParameter.Restrictions = GetRestrictions(queryString);
                    //queryParameter.QueryType = "QUERY_WITH_RESTRICTION_GREATERTHAN";
                    break;
                case 5:
                    queryParameter.GroupByFields = GetGroupByFields(queryString);
                    break;
                case 6:
                    queryParameter.OrderByFields = GetOrderByFields(queryString);
                    break;
                case 7:
                    queryParameter.GroupByFields = GetGroupByFields(queryString);
                    queryParameter.OrderByFields = GetOrderByFields(queryString);
                    break;
                case 8:
                case 9:
                case 10:
                    queryParameter.LogicalOperators = GetLogicalOperators(queryString);
                    break;
            }

            return queryParameter;
        }

        internal List<string> ParseOrderBy(string queryString)
        {
            return GetOrderByFields(queryString);
        }

        internal List<ConditionGroup> ParseCondition(string queryString)
        {
            string[] stringSplitOperators = new string[] { " where " };
            string[] whereCondition;
            whereCondition = queryString.Split(stringSplitOperators, StringSplitOptions.None);
            if (whereCondition.Length != 2)
            {
                return new List<ConditionGroup>();
            }
            return GetConditionGroupBy(whereCondition[1]);
        }
        private static List<ConditionGroup> GetConditionGroupBy(string query)
        {
            query = query.Split(new string[] { " group by ", " order by " }, StringSplitOptions.None)[0];

            string[] stringSeparators = new string[] { " or " };
            var queryGroup = query.Split(stringSeparators, StringSplitOptions.None);
            List<ConditionGroup> conditionGroups = new List<ConditionGroup>();
            stringSeparators[0] = " and ";
            foreach (var item in queryGroup)
            {
                var restrictionGroup = new List<Restriction>();
                var andGroup = item.Split(stringSeparators, StringSplitOptions.None);
                foreach (var andItem in andGroup)
                {
                    ParseCondition(restrictionGroup, andItem);
                }
                conditionGroups.Add(new ConditionGroup { ConditionsGroup = restrictionGroup, Operator = "or" });
            }

            return conditionGroups;
        }

        private static void ParseCondition(List<Restriction> restrictionGroup, string andItem)
        {
            var condition = andItem.Trim().Split(' ');

            if (condition.Length == 3)
                restrictionGroup.Add(new Restriction(condition[0], condition[2], condition[1]));
            else
            {
                if (andItem.IndexOf("==") != -1)
                {
                    SplitExpression(restrictionGroup, andItem, "==");
                }
                else if (andItem.IndexOf("!=") != -1)
                {
                    SplitExpression(restrictionGroup, andItem, "!=");
                }
                else if (andItem.IndexOf(">=") != -1)
                {
                    SplitExpression(restrictionGroup, andItem, ">=");
                }
                else if (andItem.IndexOf("<=") != -1)
                {
                    SplitExpression(restrictionGroup, andItem, "<=");
                }
                else if (andItem.IndexOf("<") != -1)
                {
                    SplitExpression(restrictionGroup, andItem, "<");
                }
                else if (andItem.IndexOf(">") != -1)
                {
                    SplitExpression(restrictionGroup, andItem, ">");
                }
                else if (andItem.IndexOf("<>") != -1)
                {
                    SplitExpression(restrictionGroup, andItem, "<>");
                }
                else if (andItem.IndexOf("=") != -1)
                {
                    SplitExpression(restrictionGroup, andItem, "=");
                }
            }
        }

        private static void SplitExpression(List<Restriction> restrictionGroup, string expression, string conditionoperator)
        {
            var item = expression.Split(new string[] { conditionoperator }, StringSplitOptions.None);
            if (item.Length == 2)
            {
                restrictionGroup.Add(new Restriction(item[0].Trim(), item[1].Trim(), conditionoperator.Trim()));
            }
        }
        /*
           * extract the name of the file from the query. File name can be found after the
           * "from" clause.
           */

        /*
         * extract the order by fields from the query string. Please note that we will
         * need to extract the field(s) after "order by" clause in the query, if at all
         * the order by clause exists. For eg: select city,winner,team1,team2 from
         * data/ipl.csv order by city from the query mentioned above, we need to extract
         * "city". Please note that we can have more than one order by fields.
         */

        /*
         * extract the group by fields from the query string. Please note that we will
         * need to extract the field(s) after "group by" clause in the query, if at all
         * the group by clause exists. For eg: select city,max(win_by_runs) from
         * data/ipl.csv group by city from the query mentioned above, we need to extract
         * "city". Please note that we can have more than one group by fields.
         */

        /*
         * extract the selected fields from the query string. Please note that we will
         * need to extract the field(s) after "select" clause followed by a space from
         * the query string. For eg: select city,win_by_runs from data/ipl.csv from the
         * query mentioned above, we need to extract "city" and "win_by_runs". Please
         * note that we might have a field containing name "from_date" or "from_hrs".
         * Hence, consider this while parsing.
         */

        /*
         * extract the conditions from the query string(if exists). for each condition,
         * we need to capture the following: 1. Name of field 2. condition 3. value
         * 
         * For eg: select city,winner,team1,team2,player_of_match from data/ipl.csv
         * where season >= 2008 or toss_decision != bat
         * 
         * here, for the first condition, "season>=2008" we need to capture: 1. Name of
         * field: season 2. condition: >= 3. value: 2008
         * 
         * the query might contain multiple conditions separated by OR/AND operators.
         * Please consider this while parsing the conditions.
         * 
         */

        /*
         * extract the logical operators(AND/OR) from the query, if at all it is
         * present. For eg: select city,winner,team1,team2,player_of_match from
         * data/ipl.csv where season >= 2008 or toss_decision != bat and city =
         * bangalore
         * 
         * the query mentioned above in the example should return a List of Strings
         * containing [or,and]
         */

        /*
         * extract the aggregate functions from the query. The presence of the aggregate
         * functions can determined if we have either "min" or "max" or "sum" or "count"
         * or "avg" followed by opening braces"(" after "select" clause in the query
         * string. in case it is present, then we will have to extract the same. For
         * each aggregate functions, we need to know the following: 1. type of aggregate
         * function(min/max/count/sum/avg) 2. field on which the aggregate function is
         * being applied
         * 
         * Please note that more than one aggregate function can be present in a query
         * 
         * 
         */

        /*
  * extract the selected fields from the query string. Please note that we will
  * need to extract the field(s) after "select" clause followed by a space from
  * the query string. For eg: select city,win_by_runs from data/ipl.csv from the
  * query mentioned above, we need to extract "city" and "win_by_runs". Please
  * note that we might have a field containing name "from_date" or "from_hrs".
  * Hence, consider this while parsing.
  */
        private List<string> GetFields(string queryString)
        {
            string[] query = queryString.Split(new char[] { ' ' });
            string queryFieldsText = string.Empty;

            for (int count = 0; count < query.Length; count++)
            {
                if (count == 1)
                {
                    queryFieldsText = query[count];
                    break;
                }
            }
            List<string> queryFields = queryFieldsText.Split(new char[] { ',' }).ToList();

            //string[] queryFields = queryFieldsText.Split(new char[] { ',' });

            foreach (string fields in queryFields)
            {
                Console.WriteLine(fields);
            }
            return queryFields;
        }

        /*
	 * extract the conditions from the query string(if exists). for each condition,
	 * we need to capture the following: 1. Name of field 2. condition 3. value
	 * 
	 * For eg: select city,winner,team1,team2,player_of_match from data/ipl.csv
	 * where season >= 2008 or toss_decision != bat
	 * 
	 * here, for the first condition, "season>=2008" we need to capture: 1. Name of
	 * field: season 2. condition: >= 3. value: 2008 Also use trim() where ever
	 * required
	 * 
	 * the query might contain multiple conditions separated by OR/AND operators.
	 * Please consider this while parsing the conditions .
	 * 
	 */

        private List<Restriction> GetRestrictions(string queryString)
        {
            string[] query = queryString.Split(new char[] { ' ' });

            if (query.Contains("group"))
            {
                queryString = queryString.Substring(0, queryString.IndexOf(" group by"));
            }
            if (query.Contains("order"))
            {
                queryString = queryString.Substring(0, queryString.IndexOf(" order by"));
            }
            query = queryString.Split(new char[] { ' ' });
            string queryCondition = string.Empty;
            StringBuilder whereCondition = new StringBuilder();
            for (int count = 0; count < query.Length; count++)
            {
                if (count > 4)
                {
                    if (query[count].Equals("group by") || query[count].Equals("order by"))
                    {
                        break;
                    }
                    whereCondition.Append(query[count] + " ");

                }
            }
            if (whereCondition.Length == 0)
            {
                return new List<Restriction>();
            }
            whereCondition = whereCondition.Replace("where ", "");
            queryCondition = whereCondition.ToString().Trim();

            string[] conditions = null;

            if (queryCondition.IndexOf("and") > 1)
            {
                conditions = queryCondition.Split("and");
                for (int count = conditions.Length - 1; count >= 0; count--)
                {
                    conditions[count] = conditions[count].Trim();
                }
            }
            else if (queryCondition.IndexOf(" or ") > 1)
            {
                conditions = queryCondition.Split(new string[] { " or " }, StringSplitOptions.None);
            }

            if (conditions == null)
            {
                conditions = new string[1];
                conditions[0] = queryCondition;
            }
            // Restriction[] restriction = new Restriction[conditions.Length];
            List<Restriction> restrictionList = new List<Restriction>();

            string[] conditionFields = null;
            int fieldCount = 0;
            string[] orConditions;
            foreach (string fields in conditions)
            {
                // if(fieldCount == 0 && conditions.Length > 1)
                if (fieldCount == 0 && fields.IndexOf(" or ") > 1)
                {
                    orConditions = fields.Split("or");
                    conditionFields = orConditions[0].Split(new char[] { ' ' });
                    restrictionList.Add(new Restriction(conditionFields[0], conditionFields[2], conditionFields[1]));
                    conditionFields = orConditions[1].Trim().Split(new char[] { ' ' });
                    restrictionList.Add(new Restriction(conditionFields[0], conditionFields[2], conditionFields[1]));
                    fieldCount++;
                    continue;
                }

                conditionFields = fields.Split(new char[] { ' ' });
                restrictionList.Add(new Restriction(conditionFields[0], conditionFields[2], conditionFields[1]));

            }
            return restrictionList;
        }

        /*
	 * extract the logical operators(AND/OR) from the query, if at all it is
	 * present. For eg: select city,winner,team1,team2,player_of_match from
	 * data/ipl.csv where season >= 2008 or toss_decision != bat and city =
	 * bangalore
	 * 
	 * the query mentioned above in the example should return a List of Strings
	 * containing [or,and]
	 */

        private List<string> GetLogicalOperators(string queryString)
        {
            List<string> logicalOperatorList = new List<string>();
            string[] query = queryString.Split(new char[] { ' ' });
            string queryFieldsText = string.Empty;
            bool isWhere = false;
            string[] logicalOperators = null;
            string logicalOperator = string.Empty;
            for (int count = 0; count < query.Length; count++)
            {

                if (query[count].Equals("where"))
                {
                    isWhere = true;
                    continue;

                }
                if (isWhere && query[count].Equals("and"))
                {
                    logicalOperator += "and" + ",";
                }

                if (isWhere && query[count].Equals("or"))
                {
                    logicalOperator += "or" + ",";
                }
            }

            if (logicalOperator.Length > 0)
            {
                logicalOperators = logicalOperator.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);
            }

            return logicalOperators.ToList();
        }

        /*
             * extract the aggregate functions from the query. The presence of the aggregate
             * functions can determined if we have either "min" or "max" or "sum" or "count"
             * or "avg" followed by opening braces"(" after "select" clause in the query
             * string. in case it is present, then we will have to extract the same. For
             * each aggregate functions, we need to know the following: 1. type of aggregate
             * function(min/max/count/sum/avg) 2. field on which the aggregate function is
             * being applied
             * 
             * Please note that more than one aggregate function can be present in a query
             * 
             * 
             */
        private List<AggregateFunction> GetAggregateFunctions(string queryString)
        {
            string[] aggregateFunctions = null;
            string[] query = queryString.Split(new char[] { ' ' });
            string function = string.Empty;
            string field = string.Empty;

            for (int count = 0; count < query.Length; count++)
            {
                if (count == 1)
                {
                    aggregateFunctions = query[count].Split(new char[] { ',' });
                    break;
                }
            }
            List<AggregateFunction> aggregateFunctionList = new List<AggregateFunction>();

            //AggregateFunction[] aggregateFunction = new AggregateFunction[aggregateFunctions.Length];

            // int aggregateCount = 0;
            foreach (string aggregateFields in aggregateFunctions)
            {
                if (aggregateFields.IndexOf('(') <= 0) continue;

                function = aggregateFields.Substring(0, aggregateFields.IndexOf('('));
                field = Regex.Match(aggregateFields, @"\(([^)]*)\)").Groups[1].Value;
                aggregateFunctionList.Add(new AggregateFunction(field, function));
                //aggregateFunction[aggregateCount] = new AggregateFunction(field, function);
                //aggregateCount++;
            }

            return aggregateFunctionList;
        }

        /*
	 * extract the order by fields from the query string. Please note that we will
	 * need to extract the field(s) after "order by" clause in the query, if at all
	 * the order by clause exists. For eg: select city,winner,team1,team2 from
	 * data/ipl.csv order by city from the query mentioned above, we need to extract
	 * "city". Please note that we can have more than one order by fields.
	 */
        private List<string> GetOrderByFields(string queryString)
        {
            List<string> orderByFieldList = new List<string>();
            string[] orderByColumns = new string[] { };
            if (queryString.IndexOf("order by") < 1) return orderByColumns.ToList();

            string[] query = queryString.Split("order by");
            if (query.Length > 1)
            {
                orderByColumns = query[1].Split(new char[] { ',' });

                for (int count = orderByColumns.Length - 1; count >= 0; count--)
                {
                    orderByColumns[count] = orderByColumns[count].Trim();
                }
            }
            return orderByColumns.ToList();
        }

        /*
	 * extract the group by fields from the query string. Please note that we will
	 * need to extract the field(s) after "group by" clause in the query, if at all
	 * the group by clause exists. For eg: select city,max(win_by_runs) from
	 * data/ipl.csv group by city from the query mentioned above, we need to extract
	 * "city". Please note that we can have more than one group by fields.
	 */
        private List<string> GetGroupByFields(string queryString)
        {
            List<string> groupByFieldList = new List<string>();
            string[] groupByColumns = null;
            if (queryString.IndexOf("group by") < 1) return groupByFieldList.ToList();


            string[] query = queryString.Split("group by");
            if (query.Length > 1)
            {
                groupByColumns = query[1].Split(new char[] { ',' });

                for (int count = groupByColumns.Length - 1; count >= 0; count--)
                {
                    groupByColumns[count] = groupByColumns[count].Trim();
                    if (groupByColumns[count].IndexOf("order by") > 1)
                    {
                        groupByColumns[count] = groupByColumns[count].Split(new char[] { ' ' })[0];
                    }
                }
            }

            return groupByColumns.ToList();
        }

        /*
        * Extract the name of the file from the query. File name can be found after a
        * space after "from" clause. Note: CSV file can contain a field that contains
        * from as a part of the column name. For eg: from_date,from_hrs etc.
        * 
        * Please consider this while extracting the file name in this method.
        */
        private string GetFileName(string query)
        {
            string[] queryString = query.Split(new char[] { ' ' });
            string fileName = string.Empty;
            for (int count = 0; count < queryString.Length; count++)
            {
                if (queryString[count].Equals("from"))
                {

                    fileName = queryString[++count];
                    break;
                }
            }
            return fileName;
        }
    }
}