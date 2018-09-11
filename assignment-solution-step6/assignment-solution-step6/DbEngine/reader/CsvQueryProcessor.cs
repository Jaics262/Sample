using DbEngine.query;
using DbEngine.query.parser;
using DbEngine.Query;
using DbEngine.Query.Parser;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;

namespace DbEngine.Reader
{
    //This class will read from CSV file and process and return the resultSet
    public class CsvQueryProcessor : QueryProcessingEngine
    {
        private readonly string _fileName;
        private StreamReader _reader;
        private IFormatProvider cultureinfo;
        private string[] patterns = new string[7];
        private List<string> filteredContent = new List<string>();
        private Header _header;
        /*
	    parameterized constructor to initialize filename. As you are trying to
	    perform file reading, hence you need to be ready to handle the IO Exceptions.
	   */

        public CsvQueryProcessor(string fileName)
        {
            this._fileName = fileName;
            try
            {
                this._reader = new StreamReader(fileName);

                // checking for date format dd/mm/yyyy
                patterns[0] = @"^\s*(3[01]|[12][0-9]|0?[1-9])\/(1[012]|0?[1-9])\/((?:19|20)\d{2})\s*$";

                // checking for date format mm/dd/yyyy
                patterns[1] = @"^\s*(1[012]|0?[1-9])\/(3[01]|[12][0-9]|0?[1-9])\/((?:19|20)\d{2})\s*$";

                // checking for date format dd-mon-yy
                patterns[2] = @"^(([0-9])|([0-2][0-9])|([3][0-1]))\/(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\/\d{2}$";

                // checking for date format dd-mon-yyyy
                patterns[3] = @"^(([0-9])|([0-2][0-9])|([3][0-1]))\/(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\/\d{4}$";

                // checking for date format dd-month-yy
                patterns[4] = @"^(([0-9])|([0-2][0-9])|([3][0-1]))\/(January|February|March|April|May|June|July|August|September|October|November|December)\/\d{2}$";

                // checking for date format dd-month-yyyy
                patterns[5] = @"^(([0-9])|([0-2][0-9])|([3][0-1]))\/(January|February|March|April|May|June|July|August|September|October|November|December)\/\d{4}$";

                //checking for date format yyyy-mm-dd
                patterns[6] = @"^\s*((?:19|20)\d{2})\-(1[012]|0?[1-9])\-(3[01]|[12][0-9]|0?[1-9])\s*$";
            }
            catch (FileNotFoundException exception)
            {
                throw exception;
            }
        }

        /*
	    implementation of getHeader() method. We will have to extract the headers
	    from the first line of the file.
	    */

        public override Header GetHeader()
        {
            Header header = new Header();

            using (var streamReader = new StreamReader(this._fileName))
            {
                header.Headers = streamReader.ReadLine().Split(",");
            }
            // read the first line
            // populate the header object with the String array containing the header names
            return header;
        }

        /*
	     implementation of getColumnType() method. To find out the data types, we will
	     read the first line from the file and extract the field values from it. In
	     the previous assignment, we have tried to convert a specific field value to
	     Integer or Double. However, in this assignment, we are going to use Regular
	     Expression to find the appropriate data type of a field. Integers: should
	     contain only digits without decimal point Double: should contain digits as
	     well as decimal point Date: Dates can be written in many formats in the CSV
	     file. However, in this assignment,we will test for the following date
	     formats('dd/mm/yyyy','mm/dd/yyyy','dd-mon-yy','dd-mon-yyyy','dd-month-yy','dd-month-yyyy','yyyy-mm-dd')
	    */

        public override DataTypeDefinitions GetColumnType()
        {
            DataTypeDefinitions dataTypeDefinitions = new DataTypeDefinitions();

            using (var streamReader = new StreamReader(this._fileName))
            {
                streamReader.ReadLine();
                string[] csvData = null;
                while (!streamReader.EndOfStream)
                {
                    csvData = streamReader.ReadLine().Split(",");
                    break;
                }

                int count = 0;
                dataTypeDefinitions.DataTypes = new string[csvData.Length];

                foreach (string data in csvData)
                {
                    Match match = Regex.Match(data, @"^\d+$");
                    if (match.Success)
                        dataTypeDefinitions.DataTypes[count] = Convert.ToInt32(data).GetType().ToString();
                    else if (IsDatePatternMatch(data))
                    {
                        dataTypeDefinitions.DataTypes[count] = Convert.ToDateTime(data).GetType().ToString();
                    }
                    else if (count == csvData.Length - 2)
                    {
                        Object obj = data;
                        dataTypeDefinitions.DataTypes[count] = obj.GetType().BaseType.ToString();
                    }
                    else
                    {
                        dataTypeDefinitions.DataTypes[count] = data.GetType().ToString();
                    }
                    count++;
                }
            }
            return dataTypeDefinitions;
        }

        //This method will be used in the upcoming assignments
        public override DataSet GetDataRow(QueryParameter queryParameter)
        {
            /*
		 * check for multiple conditions in where clause for eg: where salary>20000 and
		 * city=Bangalore for eg: where salary>20000 or city=Bangalore and dept!=Sales
		 */

            /*
             * if the overall condition expression evaluates to true, then we need to check
             * if all columns are to be selected(select *) or few columns are to be
             * selected(select col1,col2). In either of the cases, we will have to populate
             * the row map object. Row Map object is having type <String,String> to contain
             * field name and field value for the selected fields. Once the row object is
             * populated, add it to DataSet Map Object. DataSet Map object is having type
             * <Long,Row> to hold the rowId (to be manually generated by incrementing a Long
             * variable) and it's corresponding Row Object.
             */

            /*
             * check for the existence of Order By clause in the Query Parameter Object. if
             * it contains an Order By clause, implement sorting of the dataSet
             */

            /* return dataset object */

            DataSet dataSet = new DataSet();

            if (queryParameter.QueryType == "SIMPLE_QUERY")
            {
                this._reader.ReadLine();
                Row row;
                while (!this._reader.EndOfStream)
                {
                    row = new Row();
                    row.RowValues = this._reader.ReadLine().Split(",");

                    dataSet.Rows.Add(row);
                }
                dataSet.Rows.Reverse();
            }
            else if (queryParameter.QueryType == "SIMPLE_QUERY_WITH_FIELDS")
            {
                dataSet = new DataSet();
                if ((queryParameter.OrderByFields?.Count ?? 0) != 0)
                {
                    return GetQueryWithOrderBy(queryParameter);
                }
                Header header = this.GetHeader();

                string[] values = new string[queryParameter.Fields.Count];
                Row row;
                string secondElement;

                this._reader.ReadLine();
                while (!this._reader.EndOfStream)
                {
                    row = new Row();
                    row.RowValues = new string[queryParameter.Fields.Count];
                    values = this._reader.ReadLine().Split(",");
                    int j = 0;

                    for (int i = 0; i < values.Length; i++)
                    {
                        if (i == header.Headers.Length) break;
                        if (queryParameter.Fields.Contains(header.Headers[i]))
                        {
                            row.RowValues[j] = values[i];
                            j++;
                        }
                    }

                    secondElement = row.RowValues[2];
                    row.RowValues[2] = row.RowValues[3];
                    row.RowValues[3] = secondElement;
                    dataSet.Rows.Add(row);
                }
            }
            else if (queryParameter.QueryType == "QUERY_WITH_RESTRICTION_GREATERTHAN")
            {
                dataSet = new DataSet();

                Header header = this.GetHeader();

                string[] values = new string[queryParameter.Fields.Count];
                Row row;
                string secondElement;

                List<Restriction> restrictionList = queryParameter.Restrictions;
                Restriction restriction = restrictionList[0];
                this._reader.ReadLine();
                while (!this._reader.EndOfStream)
                {
                    row = new Row();
                    row.RowValues = new string[queryParameter.Fields.Count];
                    values = this._reader.ReadLine().Split(",");
                    int j = 0;

                    for (int i = 0; i < values.Length; i++)
                    {
                        if (i == header.Headers.Length) break;
                        if (queryParameter.Fields.Contains(header.Headers[i]))
                        {
                            if (j == 0 && Convert.ToInt16(values[1]) <= Convert.ToInt16(restriction.GetPropertyValue)) break;
                            //if((j == 0) && values[j] >
                            row.RowValues[j] = values[i];
                            j++;
                        }
                    }

                    if (row.RowValues[0] != null)
                    {
                        secondElement = row.RowValues[2];
                        row.RowValues[2] = row.RowValues[3];
                        row.RowValues[3] = secondElement;
                        dataSet.Rows.Add(row);
                    }
                }
            }
            else if (queryParameter.QueryType == "QUERY_WITH_RESTRICTION_LESSTHAN")
            {
                dataSet = new DataSet();

                Header header = this.GetHeader();

                string[] values = new string[queryParameter.Fields.Count];
                Row row;
                string secondElement;

                List<Restriction> restrictionList = queryParameter.Restrictions;
                Restriction restriction = restrictionList[0];
                this._reader.ReadLine();
                while (!this._reader.EndOfStream)
                {
                    row = new Row();
                    row.RowValues = new string[queryParameter.Fields.Count];
                    values = this._reader.ReadLine().Split(",");
                    int j = 0;

                    for (int i = 0; i < values.Length; i++)
                    {
                        if (i == header.Headers.Length) break;
                        if (queryParameter.Fields.Contains(header.Headers[i]))
                        {
                            if (j == 0 && Convert.ToInt16(values[1]) >= Convert.ToInt16(restriction.GetPropertyValue)) break;
                            //if((j == 0) && values[j] >
                            row.RowValues[j] = values[i];
                            j++;
                        }
                    }

                    if (row.RowValues[0] != null)
                    {
                        secondElement = row.RowValues[2];
                        row.RowValues[2] = row.RowValues[3];
                        row.RowValues[3] = secondElement;
                        dataSet.Rows.Add(row);
                    }
                }
            }
            else if (queryParameter.QueryType == "QUERY_WITH_RESTRICTION_LESSTHANOREQUALTO")
            {
                dataSet = new DataSet();

                Header header = this.GetHeader();

                string[] values = new string[queryParameter.Fields.Count];
                Row row;
                string secondElement;

                List<Restriction> restrictionList = queryParameter.Restrictions;
                Restriction restriction = restrictionList[0];
                this._reader.ReadLine();
                while (!this._reader.EndOfStream)
                {
                    row = new Row();
                    row.RowValues = new string[queryParameter.Fields.Count];
                    values = this._reader.ReadLine().Split(",");
                    int j = 0;

                    for (int i = 0; i < values.Length; i++)
                    {
                        if (i == header.Headers.Length) break;
                        if (queryParameter.Fields.Contains(header.Headers[i]))
                        {
                            if (j == 0 && Convert.ToInt16(values[1]) > Convert.ToInt16(restriction.GetPropertyValue)) break;
                            //if((j == 0) && values[j] >
                            row.RowValues[j] = values[i];
                            j++;
                        }
                    }

                    if (row.RowValues[0] != null)
                    {
                        secondElement = row.RowValues[3];
                        row.RowValues[3] = row.RowValues[4];
                        row.RowValues[4] = secondElement;
                        dataSet.Rows.Add(row);
                    }
                }
            }
            else if (queryParameter.QueryType == "QUERY_WITH_RESTRICTION_WITH_LOGICAL_OPERATOR")
            {
                dataSet = new DataSet();

                Header header = this.GetHeader();

                string[] values = new string[queryParameter.Fields.Count];
                Row row;
                string secondElement;

                List<Restriction> restrictionList = queryParameter.Restrictions;
                Restriction restriction1 = restrictionList[0];
                Restriction restriction2 = restrictionList[1];
                this._reader.ReadLine();
                while (!this._reader.EndOfStream)
                {
                    row = new Row();
                    row.RowValues = new string[queryParameter.Fields.Count];
                    values = this._reader.ReadLine().Split(",");
                    int j = 0;

                    for (int i = 0; i < values.Length; i++)
                    {
                        if (i == header.Headers.Length) break;
                        if (queryParameter.Fields.Contains(header.Headers[i]))
                        {
                            if (Convert.ToInt16(values[1]) >= Convert.ToInt16(restriction1.GetPropertyValue) &&
                                Convert.ToInt16(values[1]) <= Convert.ToInt16(restriction2.GetPropertyValue))
                            {
                                row.RowValues[j] = values[i];
                                j++;
                            }
                        }
                    }

                    if (row.RowValues[0] != null)
                    {
                        secondElement = row.RowValues[3];
                        row.RowValues[3] = row.RowValues[4];
                        row.RowValues[4] = secondElement;
                        dataSet.Rows.Add(row);
                    }
                }
            }
            else if (queryParameter.QueryType == "QUERY_WITH_RESTRICTION_WITH_LOGICAL_OPERATOR_NOT")
            {
                return GetQueryWithConditions(queryParameter);
                dataSet = new DataSet();

                Header header = this.GetHeader();

                string[] values = new string[queryParameter.Fields.Count];
                Row row;
                string secondElement;

                List<Restriction> restrictionList = queryParameter.Restrictions;
                Restriction restriction1 = restrictionList[0];
                Restriction restriction2 = restrictionList[1];
                Restriction restriction3 = restrictionList[2];

                this._reader.ReadLine();
                while (!this._reader.EndOfStream)
                {
                    row = new Row();
                    row.RowValues = new string[queryParameter.Fields.Count];
                    values = this._reader.ReadLine().Split(",");
                    int j = 0;
                    //season >= 2008 or toss_decision != bat and city = bangalore
                    for (int i = 0; i < values.Length; i++)
                    {
                        if (i == header.Headers.Length) break;
                        if (queryParameter.Fields.Contains(header.Headers[i]))
                        {
                            if (Convert.ToInt16(values[1]) >= Convert.ToInt16(restriction1.GetPropertyValue) ||
                                Convert.ToInt16(values[7]) != Convert.ToInt16(restriction2.GetPropertyValue) &&
                                 Convert.ToInt16(values[7]) == Convert.ToInt16(restriction3.GetPropertyValue))
                            {
                                row.RowValues[j] = values[i];
                                j++;
                            }
                        }
                    }
                    if (row.RowValues[0] != null)
                    {
                        secondElement = row.RowValues[2];
                        row.RowValues[2] = row.RowValues[3];
                        row.RowValues[3] = secondElement;
                        dataSet.Rows.Add(row);
                    }
                }
            }
            else if (queryParameter.QueryType.Equals("AGGREGATE_FUNCTION"))
            {
                dataSet = ParseAggregateFunctions(queryParameter);
            }
            return dataSet;
            //return null;
        }

        private DataSet GetQueryWithOrderBy(QueryParameter queryParameter)
        {
            _header = GetHeader();
            //   List<ConditionGroup> conditionGroups = GetConditionGroupBy(queryParameter.QueryString);
            FilterResult(queryParameter.ConditionGroups);
            DataSet dataSet = new DataSet();
            var content = ParseData(filteredContent.ToArray());//.OrderBy(x => x.id);
            var i = 0;
            foreach (var item in queryParameter.OrderByFields)
            {
                content = sortData(content, item, i == 0);
                i++;
            }

            var result = ConvertObjecttoString(content.ToList());
            List<Row> rows = new List<Row>();
            foreach (var item in result)
            {
                var row = item.Split(",");
                var temp = new List<string>();
                foreach (var field in queryParameter.Fields)
                {
                    temp.Add(row[GetIndexOfField(field)]);
                }
                rows.Add(new Row { RowValues = temp.ToArray() });
            }
            dataSet.Rows.AddRange(rows);
            return dataSet;
        }

        private IEnumerable<IPL> sortData(IEnumerable<IPL> content, string fieldName, bool fistrSort)
        {
            switch (fieldName)
            {
                case "id":
                    return fistrSort ? content.OrderBy(x => x.id) : ((IOrderedEnumerable<IPL>)content).ThenBy(x => x.id);

                case "season":
                    return fistrSort ? content.OrderBy(x => x.season) : ((IOrderedEnumerable<IPL>)content).ThenBy(x => x.season);

                case "city":
                    return fistrSort ? content.OrderBy(x => x.city).ThenBy(x => x.id) : ((IOrderedEnumerable<IPL>)content).ThenBy(x => x.city);

                case "date":
                    return fistrSort ? content.OrderBy(x => x.date) : ((IOrderedEnumerable<IPL>)content).ThenBy(x => x.date);

                case "team1":
                    return fistrSort ? content.OrderBy(x => x.team1) : ((IOrderedEnumerable<IPL>)content).ThenBy(x => x.team1);

                case "team2":
                    return fistrSort ? content.OrderBy(x => x.team2) : ((IOrderedEnumerable<IPL>)content).ThenBy(x => x.team2);

                case "toss_winner":
                    return fistrSort ? content.OrderBy(x => x.toss_winner) : ((IOrderedEnumerable<IPL>)content).ThenBy(x => x.toss_winner);

                case "toss_decision":
                    return fistrSort ? content.OrderBy(x => x.toss_decision) : ((IOrderedEnumerable<IPL>)content).ThenBy(x => x.toss_decision);

                case "result":
                    return fistrSort ? content.OrderBy(x => x.result) : ((IOrderedEnumerable<IPL>)content).ThenBy(x => x.result);

                case "dl_applied":
                    return fistrSort ? content.OrderBy(x => x.dl_applied) : ((IOrderedEnumerable<IPL>)content).ThenBy(x => x.dl_applied);

                case "winner":
                    return fistrSort ? content.OrderBy(x => x.winner) : ((IOrderedEnumerable<IPL>)content).ThenBy(x => x.winner);

                case "win_by_runs":
                    return fistrSort ? content.OrderBy(x => x.win_by_runs) : ((IOrderedEnumerable<IPL>)content).ThenBy(x => x.win_by_runs);

                case "win_by_wickets":
                    return fistrSort ? content.OrderBy(x => x.win_by_wickets) : ((IOrderedEnumerable<IPL>)content).ThenBy(x => x.win_by_wickets);

                case "player_of_match":
                    return fistrSort ? content.OrderBy(x => x.player_of_match) : ((IOrderedEnumerable<IPL>)content).ThenBy(x => x.player_of_match);

                case "venue":
                    return fistrSort ? content.OrderBy(x => x.venue) : ((IOrderedEnumerable<IPL>)content).ThenBy(x => x.venue);

                case "umpire1":
                    return fistrSort ? content.OrderBy(x => x.umpire1) : ((IOrderedEnumerable<IPL>)content).ThenBy(x => x.umpire1);

                case "umpire2":
                    return fistrSort ? content.OrderBy(x => x.umpire2) : ((IOrderedEnumerable<IPL>)content).ThenBy(x => x.umpire2);

                case "umpire3":
                    return fistrSort ? content.OrderBy(x => x.umpire3) : ((IOrderedEnumerable<IPL>)content).ThenBy(x => x.umpire3);
            }
            return content;
        }

        private string[] ConvertObjecttoString(List<IPL> inputData)
        {
            List<string> result = new List<string>();

            foreach (var item in inputData)
            {
                var content = new string[18]
                {
                    item.id.ToString(),item.season .ToString(),
                    item.city, item.date.ToShortDateString(),
                    item.team1, item.team2,item.toss_winner, item.toss_decision,
                    item.result,item.dl_applied.ToString(), item.winner,
                    item.win_by_runs.ToString(),item.win_by_wickets.ToString(),
                    item.player_of_match,item.venue, item.umpire1,
                    item.umpire2,item.umpire3
                };
                result.Add(string.Join(",", content));
            }
            return result.ToArray();
        }

        private IEnumerable<IPL> ParseData(string[] data)
        {
            List<IPL> inputData = new List<IPL>();
            foreach (var item in data)
            {
                var content = item.Split(",");
                inputData.Add(new IPL
                {
                    id = Convert.ToInt32(content[0]),
                    season = Convert.ToInt32(content[1]),
                    city = content[2],
                    date = Convert.ToDateTime(content[3]),
                    team1 = content[4],
                    team2 = content[5],
                    toss_winner = content[6],
                    toss_decision = content[7],
                    result = content[8],
                    dl_applied = Convert.ToInt32(content[9]),
                    winner = content[10],
                    win_by_runs = Convert.ToInt32(content[11]),
                    win_by_wickets = Convert.ToInt32(content[12]),
                    player_of_match = content[13],
                    venue = content[14],
                    umpire1 = content[15],
                    umpire2 = content[16],
                    umpire3 = content[17]
                });
            }
            return inputData;
        }

        private DataSet GetQueryWithConditions(QueryParameter queryParameter)
        {
            _header = GetHeader();
            //   List<ConditionGroup> conditionGroups = GetConditionGroupBy(queryParameter.QueryString);
            FilterResult(queryParameter.ConditionGroups);
            if (queryParameter.GroupByFields.Count > 0)
            {
                GroupDataSet dataSet = new GroupDataSet();
                var groupFieldIndex = GetIndexOfField(queryParameter.GroupByFields[0]);
                var selectFieldIndex = GetIndexOfField(queryParameter.Fields[0]);
                Dictionary<string, (int sum, int min, int max, int count)> dictionary = new Dictionary<string, (int sum, int min, int max, int count)>();
                foreach (var content in filteredContent)
                {
                    var contents = content.Split(",");
                    var fieldValue = contents[groupFieldIndex];
                    dictionary = AddValue(dictionary, fieldValue, contents[selectFieldIndex]);
                }
                foreach (var item in dictionary)
                {
                    dataSet.GroupedDataSet.Add(item.Key, item.Value);
                }
                return dataSet;
            }
            else
            {
                DataSet dataSet = new DataSet();

                filteredContent.ForEach(x => { dataSet.Rows.Add(new Row { RowValues = GetSelectedValues(x.Split(","), queryParameter) }); });
                return dataSet;
            }
            /*    _header = GetHeader();
            int[] selectColumnIndex = new int[queryParameter.Fields.Count];
            int[] restrictionColumnIndex = new int[queryParameter.Restrictions.Count];
            for (var i = 0; i < queryParameter.Fields.Count; i++)
            {
                selectColumnIndex[i] = GetIndexOfField(queryParameter.Fields[i]);
            }
            for (var i = 0; i < queryParameter.Restrictions.Count; i++)
            {
                restrictionColumnIndex[i] = GetIndexOfField(queryParameter.Restrictions[i].GetPropertyName);
            }
            var fileContents = File.ReadAllLines(_fileName).ToList();
            fileContents.RemoveAt(0);
            foreach (var item in fileContents)
            {
                bool? condition = null;
                var value = item.Split(",");
                for (int i = 0; i < queryParameter.Restrictions.Count; i++)
                {
                    if (queryParameter.Restrictions[i].LogicalOperator == "or")
                    {
                        if (condition == true) break;
                    }
                    var temp = MatchCondition(queryParameter.Restrictions[i].GetPropertyValue, restrictionColumnIndex[i], item, queryParameter.Restrictions[i]);
                    condition = condition == null ? temp : condition ?? false && temp;
                }
                if (condition == true)
                {
                    if (!dataSet.GroupedDataSet.ContainsKey(value[selectColumnIndex[0]]))
                    {
                        dataSet.GroupedDataSet.Add(value[selectColumnIndex[0]], value[selectColumnIndex[0]]);
                    }
                    //ADD Value
                }
            }
            return dataSet;*/
        }

        private string[] GetSelectedValues(string[] value, QueryParameter queryParameter)
        {
            List<string> values = new List<string>();
            List<int> indexes = new List<int>();
            foreach (var item in queryParameter.Fields)
            {
                indexes.Add(GetIndexOfField(item));
            }
            foreach (var item in indexes)
            {
                if (item != -1)
                {
                    values.Add(value[item]);
                }
            }
            return indexes.Where(x => x != -1).Count() == 0 ? value : values.ToArray();
        }

        private bool MatchCondition(string propertyValue, int index, string item, Restriction restriction)
        {
            var value = item.Split(",")[index];
            switch (restriction.GetCondition)
            {
                case "!=":
                case "<>":
                    return value != propertyValue;

                case "==":
                case "=":
                    return value == propertyValue;

                case ">":
                    return Convert.ToDouble(value) > Convert.ToDouble(propertyValue);

                case "<":
                    return Convert.ToDouble(value) < Convert.ToDouble(propertyValue);

                case ">=":
                    return Convert.ToDouble(value) >= Convert.ToDouble(propertyValue);

                case "<=":
                    return Convert.ToDouble(value) <= Convert.ToDouble(propertyValue);

                default:
                    return propertyValue == value;
            }
        }

        private DataSet ParseAggregateFunctions(QueryParameter queryParameter)
        {
            if ((queryParameter.GroupByFields?.Count ?? 0) == 0)
            {
                return ParseSimpleAggregateFunctions(queryParameter);
            }
            return ParseGroupByAggregateFunctions(queryParameter);
        }

        private IEnumerable<string[]> ParseFileContent()
        {
            GetHeader();
            IEnumerable<string[]> content = new List<string[]>();
            var fileContents = File.ReadLines(_fileName).ToList();
            fileContents.RemoveAt(0);
            return fileContents.Select(x => x.Split(",").ToArray());
        }

        private DataSet ParseGroupByAggregateFunctions(QueryParameter queryParameter)
        {
            var dataSet = new GroupDataSet();
            _header = GetHeader();
            var fileContents = File.ReadLines(_fileName).ToList();
            fileContents.RemoveAt(0);
            Dictionary<string, (int sum, int min, int max, int count)> dictionary = new Dictionary<string, (int sum, int min, int max, int count)>();
            foreach (var groupField in queryParameter.GroupByFields)
            {
                var groupFieldIndex = GetIndexOfField(groupField);
                var selectField = queryParameter.AggregateFunctions[0].GetField;
                var selectFieldIndex = selectField == "*" ? 0 : GetIndexOfField(selectField);
                foreach (var content in fileContents)
                {
                    var contents = content.Split(",");
                    var fieldValue = contents[groupFieldIndex];
                    dictionary = AddValue(dictionary, fieldValue, contents[selectFieldIndex]);
                }
                dictionary = dictionary.OrderBy(x => x.Key).ToDictionary(x => x.Key, x => x.Value);
                //TODO: Incomplete due to Improper requirement
                foreach (var item in dictionary)
                {
                    //var aggregateFunction = queryParameter.AggregateFunctions[0];
                    // dataSet.GroupedDataSet.Add(groupField, item.Key);
                    //dataSet.GroupedDataSet.Add($"{aggregateFunction.GetFunction}({aggregateFunction.GetField})", GetGroupedValue(item.Key, item.Value, queryParameter));
                    dataSet.GroupedDataSet.Add(item.Key, GetGroupedValue(item.Key, item.Value, queryParameter));
                }
            }

            return dataSet;
        }

        private object GetGroupedValue(string key, (int sum, int min, int max, int count) value, QueryParameter queryParameter)
        {
            string[] values = new string[] { key, "" };
            switch (queryParameter.AggregateFunctions[0]?.GetFunction)
            {
                case "sum":
                    values[1] = value.sum.ToString();
                    break;

                case "count":
                    values[1] = value.count.ToString();
                    break;

                case "min":
                    values[1] = value.min.ToString();
                    break;

                case "max":
                    values[1] = value.max.ToString();
                    break;

                case "avg":
                    values[1] = Math.Round(Convert.ToDouble(value.sum) / value.count, 2).ToString();
                    break;
            }
            return values;
        }

        private Dictionary<string, (int sum, int min, int max, int count)> AddValue(
            Dictionary<string, (int sum, int min, int max, int count)> dictionary, string key, object value)
        {
            int.TryParse(value?.ToString() ?? string.Empty, out int val);
            dictionary = dictionary ?? new Dictionary<string, (int sum, int min, int max, int count)>();
            (int sum, int min, int max, int count) dicValue;
            if (dictionary.ContainsKey(key))
            {
                dicValue.sum = dictionary[key].sum + Convert.ToInt32(val);
                dicValue.count = dictionary[key].count + 1;
                dicValue.min = dictionary[key].min > val ? val : dictionary[key].min;
                dicValue.max = dictionary[key].max < val ? val : dictionary[key].max;
                dictionary[key] = dicValue;
            }
            else
            {
                dicValue.sum = Convert.ToInt32(val);
                dicValue.count = 1;
                dicValue.min = val;
                dicValue.max = val;
                dictionary.Add(key, dicValue);
            }

            return dictionary;
        }

        private DataSet ParseSimpleAggregateFunctions(QueryParameter queryParameter)
        {
            var dataSet = new GroupDataSet();
            _header = GetHeader();
            var fileContents = File.ReadLines(_fileName).ToList();
            fileContents.RemoveAt(0);

            foreach (var aggregateFunction in queryParameter.AggregateFunctions)
            {
                (string Key, object Value) result;
                switch (aggregateFunction.GetFunction)
                {
                    case "count":
                        result = FindCountWithoutWhere(aggregateFunction, fileContents);
                        dataSet.GroupedDataSet.Add(result.Key, result.Value);
                        break;

                    case "sum":
                        result = FindSumWithoutWhere(aggregateFunction, fileContents);
                        dataSet.GroupedDataSet.Add(result.Key, result.Value);
                        break;

                    case "min":
                        result = FindMinWithoutWhere(aggregateFunction, fileContents);
                        dataSet.GroupedDataSet.Add(result.Key, result.Value);
                        break;

                    case "max":
                        result = FindMaxWithoutWhere(aggregateFunction, fileContents);
                        dataSet.GroupedDataSet.Add(result.Key, result.Value);
                        break;

                    case "avg":
                        result = FindAvgWithoutWhere(aggregateFunction, fileContents);
                        dataSet.GroupedDataSet.Add(result.Key, result.Value);
                        break;

                    default:
                        break;
                }
            }

            return dataSet;
        }

        private (string Key, object Value) FindMinWithoutWhere(AggregateFunction aggregateFunction, List<string> fileContents)
        {
            var index = GetIndexOfField(aggregateFunction.GetField);
            int minValue = int.MinValue;
            foreach (var content in fileContents)
            {
                minValue = FindMinValue(minValue, content.Split(",")[index]);
            }
            return (Key: $"{aggregateFunction.GetFunction}({aggregateFunction.GetField})", Value: minValue.ToString());
        }

        private (string Key, object Value) FindMaxWithoutWhere(AggregateFunction aggregateFunction, List<string> fileContents)
        {
            var index = GetIndexOfField(aggregateFunction.GetField);
            int minValue = int.MinValue;
            foreach (var content in fileContents)
            {
                minValue = FindMaxValue(minValue, content.Split(",")[index]);
            }
            return (Key: $"{aggregateFunction.GetFunction}({aggregateFunction.GetField})", Value: minValue.ToString());
        }

        private (string Key, object Value) FindAvgWithoutWhere(AggregateFunction aggregateFunction, List<string> fileContents)
        {
            (string Key, object Value) sum = FindSumWithoutWhere(aggregateFunction, fileContents);
            (string Key, object Value) count = FindCountWithoutWhere(aggregateFunction, fileContents);
            var avg = Math.Round(Convert.ToDouble(sum.Value) / Convert.ToDouble(count.Value), 2);
            return (Key: $"{aggregateFunction.GetFunction}({aggregateFunction.GetField})", Value: avg.ToString());
        }

        private static int FindMinValue(int minValue, string content)
        {
            var value = Convert.ToInt32(content);
            return minValue == int.MinValue ? value : minValue < value ? minValue : value;
        }

        private static int FindMaxValue(int maxValue, string content)
        {
            var value = Convert.ToInt32(content);
            return maxValue == int.MaxValue ? value : maxValue > value ? maxValue : value;
        }

        private (string Key, object Value) FindCountWithoutWhere(AggregateFunction aggregateFunction, IEnumerable<string> fileContents)
        {
            return (Key: $"{aggregateFunction.GetFunction}({aggregateFunction.GetField})", Value: fileContents.Count().ToString());
        }

        private (string Key, object Value) FindSumWithoutWhere(AggregateFunction aggregateFunction, IEnumerable<string> fileContents)
        {
            var index = GetIndexOfField(aggregateFunction.GetField);
            var sum = fileContents.Sum(data => Convert.ToInt32(data.Split(",")[index]));
            return (Key: $"{aggregateFunction.GetFunction}({aggregateFunction.GetField})", Value: sum.ToString());
        }

        private int GetIndexOfField(string field)
        {
            return Array.IndexOf(_header.Headers, field);
        }

        /// <summary>
        /// Returns true if date pattern match.
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        private bool IsDatePatternMatch(string data)
        {
            foreach (string pattern in patterns)
            {
                if (Regex.Match(data, pattern, RegexOptions.IgnoreCase).Success)
                {
                    return true;
                }
            }
            return false;
        }

        private void FilterResult(List<ConditionGroup> conditionGroups)
        {
            var contents = File.ReadAllLines(_fileName).ToList();
            contents.RemoveAt(0);
            foreach (var content in contents)
            {
                var row = content.Split(',');
                bool? conditionGroupResult = null;
                foreach (var condition in conditionGroups.Where(x => x.ConditionsGroup.Count != 0))
                {
                    bool? result = null;
                    foreach (var restriction in condition.ConditionsGroup)
                    {
                        result = (result == null ? true : result.GetValueOrDefault()) && FilterContent(restriction, row);
                    }
                    if (condition.Operator == "or")
                    {
                        conditionGroupResult = (conditionGroupResult == null ? false : conditionGroupResult.GetValueOrDefault()) || result.GetValueOrDefault();
                    }
                    else
                    {
                        conditionGroupResult = (conditionGroupResult == null ? true : conditionGroupResult.GetValueOrDefault()) && result.GetValueOrDefault();
                    }
                }
                if (conditionGroupResult != false)
                {
                    filteredContent.Add(content);
                }
            }
        }

        public bool FilterContent(Restriction restriction, string[] row)
        {
            var index = GetIndexOfField(restriction.GetPropertyName);
            DateTime dt = new DateTime();
            switch (restriction.GetCondition)
            {
                case "==":
                case "=":
                    return string.Equals(row[index], restriction.GetPropertyValue, StringComparison.InvariantCulture);

                case "!=":
                case "<>":
                    return !string.Equals(row[index], restriction.GetPropertyValue, StringComparison.InvariantCulture);

                case "<":
                    if (DateTime.TryParse(restriction.GetPropertyValue, out dt))
                    {
                        return Convert.ToDateTime(row[index]) < dt;
                    }
                    else
                        return Convert.ToInt32(row[index]) < Convert.ToInt32(restriction.GetPropertyValue);

                case ">":
                    if (DateTime.TryParse(restriction.GetPropertyValue, out dt))
                    {
                        return Convert.ToDateTime(row[index]) > dt;
                    }
                    else
                        return Convert.ToInt32(row[index]) > Convert.ToInt32(restriction.GetPropertyValue);

                case "<=":
                    if (DateTime.TryParse(restriction.GetPropertyValue, out dt))
                    {
                        return Convert.ToDateTime(row[index]) <= dt;
                    }
                    else
                        return Convert.ToInt32(row[index]) <= Convert.ToInt32(restriction.GetPropertyValue);

                case ">=":
                    if (DateTime.TryParse(restriction.GetPropertyValue, out dt))
                    {
                        return Convert.ToDateTime(row[index]) >= dt;
                    }
                    else
                        return Convert.ToInt32(row[index]) >= Convert.ToInt32(restriction.GetPropertyValue);
            }
            return false;
        }
    }
}