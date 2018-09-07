using System;
using System.Collections.Generic;
using DbEngine.Query;
using System.Text.RegularExpressions;
using System.IO;
using System.Globalization;
using DbEngine.Query.Parser;

namespace DbEngine.Reader
{
    /* This class is used for evaluating queries with aggregate functions and group by clause*/
    class CsvGroupByAggregateQueryProcessor : QueryProcessingEngine
    {

        /*
	    parameterized constructor to initialize filename. As you are trying to perform file reading, hence you need to be ready to handle the IO Exceptions.
	   */
        public CsvGroupByAggregateQueryProcessor(string fileName)
        {
        }

        /*
	    implementation of getHeader() method. We will have to extract the headers from the first line of the file.
	    */
        public override Header GetHeader()
        {
            return null;
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
            return null;
        }

        //This method will be used in the upcoming assignments
        public override DataSet GetDataRow(QueryParameter queryParameter)
        {

            /*
              * from QueryParameter object, read one condition at a time and evaluate the
              * same. For evaluating the conditions, we will use evaluateExpressions() method
              * of Filter class. Please note that evaluation of expression will be done
              * differently based on the data type of the field. In case the query is having
              * multiple conditions, you need to evaluate the overall expression i.e. if we
              * have OR operator between two conditions, then the row will be selected if any
              * of the condition is satisfied. However, in case of AND operator, the row will
              * be selected only if both of them are satisfied.
              */


            /*
		 if the overall condition expression evaluates to true, then we need to check for the existence for group by clause in the Query Parameter. The dataSet generated after processing a group by with aggregate clause is completely different from a dataSet structure(which contains multiple rows of data). In case of queries containing group by clause and aggregate functions, the resultSet will contain multiple dataSets, each of which will be assigned to
		   the group by column value i.e. for all unique values of the group by column, aggregates will have to be calculated. Hence, we will use GroupedDataSet to store the same and not DataSet
		  Please note we will process queries containing one group by column only for this example.
		 */
            return null;
        }
    }
}
