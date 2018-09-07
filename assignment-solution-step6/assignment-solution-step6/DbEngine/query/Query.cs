using DbEngine.Query.Parser;
using DbEngine.Reader;
using DbEngine.Query;
using System.IO;
using System;

namespace DbEngine.Query
{
    public class Query
    {
        QueryParser queryParser = null;
        QueryParameter queryParameter = null;
        CsvQueryProcessor queryProcessor = null;

        /*
	 * This method will: 
	 * 1.parse the query and populate the QueryParameter object
	 * 2.Based on the type of query, it will select the appropriate Query processor.
	 * 
	 * In this example, we are going to work with only one Query Processor which is
	 * CsvQueryProcessor, which can work with select queries containing zero, one or
	 * multiple conditions
	 */

        public DataSet ExecuteQuery(string queryString)
        {
            queryParser = new QueryParser();
            queryParameter = queryParser.ParseQuery(queryString, 3);
            queryProcessor = new CsvQueryProcessor(Path.Combine(Environment.CurrentDirectory, @queryParameter.File));
            DataSet dataSet = new DataSet();
            queryParameter = queryParser.ParseQuery(queryString, 2);
            queryParameter = queryParser.ParseQuery(queryString, 1);
            if (queryParameter.AggregateFunctions.Count == 0)
            {
                queryParameter = queryParser.ParseQuery(queryString, 4);
            }
            queryParameter = queryParser.ParseQuery(queryString, 5);
            

            if (queryParameter.Fields[0] == "*")
            {
                queryParameter.QueryType = "SIMPLE_QUERY";
                dataSet = queryProcessor.GetDataRow(queryParameter);
            }
            else if(queryParameter.AggregateFunctions.Count > 0)
            {
                //if(queryParameter.AggregateFunctions[0].GetFunction() == ")
                queryParameter.QueryType = "AGGREGATE_FUNCTION";
                dataSet = queryProcessor.GetDataRow(queryParameter);
            }
            else
            {
                if (queryParameter.Fields.Count > 0 && queryParameter.Restrictions.Count == 0)
                {
                    queryParameter.QueryType = "SIMPLE_QUERY_WITH_FIELDS";
                    dataSet = queryProcessor.GetDataRow(queryParameter);
                }
                //if()
                else
                {
                    if (queryParameter.Restrictions.Count > 0 && queryParameter.Restrictions[0].GetCondition == ">")
                    {

                        queryParameter.QueryType = "QUERY_WITH_RESTRICTION_GREATERTHAN";
                        dataSet = queryProcessor.GetDataRow(queryParameter);
                    }
                    else if (queryParameter.Restrictions.Count > 0 && queryParameter.Restrictions[0].GetCondition == "<")
                    {
                        queryParameter.QueryType = "QUERY_WITH_RESTRICTION_LESSTHAN";
                        dataSet = queryProcessor.GetDataRow(queryParameter);
                    }
                    else if (queryParameter.Restrictions.Count > 0 && queryParameter.Restrictions[0].GetCondition == "<=")
                    {
                        queryParameter.QueryType = "QUERY_WITH_RESTRICTION_LESSTHANOREQUALTO";
                        dataSet = queryProcessor.GetDataRow(queryParameter);
                    }
                    else
                    {
                        queryParameter = queryParser.ParseQuery(queryString, 10);
                        if (queryParameter.LogicalOperators.Count > 1)
                        {
                            queryParameter.QueryType = "QUERY_WITH_RESTRICTION_WITH_LOGICAL_OPERATOR_NOT";
                            dataSet = queryProcessor.GetDataRow(queryParameter);
                        }
                        else
                        {
                            queryParameter.QueryType = "QUERY_WITH_RESTRICTION_WITH_LOGICAL_OPERATOR";
                            dataSet = queryProcessor.GetDataRow(queryParameter);
                        }
                    }
                }
            }
            return dataSet;
        }
        // return new DataSet();



        /* instantiate QueryParser class */

        /* call parseQuery() method of the class by passing the queryString which will return object of QueryParameter
         */


        /*
         * Check for Type of Query based on the QueryParameter object. In this assignment, we will process only queries containing zero, one or multiple where conditions i.e. conditions without aggregate functions, order by clause or group by clause
         */



        /*
         call the GetDataRow() method of CsvQueryProcessor class by passing the QueryParameter Object to it. This method is supposed to return DataSet
         */
        // return null;
    }
    }


