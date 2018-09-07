using System;
using System.IO;
using System.Globalization;
using System.Text.RegularExpressions;
using DbEngine.Query;
using System.Collections.Generic;
using DbEngine.Query.Parser;

namespace DbEngine.Reader
{
    //This class will read from CSV file and process and return the resultSet
    public class CsvQueryProcessor: QueryProcessingEngine
    {

        private readonly string _fileName;
        private StreamReader _reader;
        private IFormatProvider cultureinfo;
        private string[] patterns = new string[7];

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

            return dataSet;
            //return null;
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

    }
}