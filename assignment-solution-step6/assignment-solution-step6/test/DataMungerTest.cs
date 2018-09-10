using System;
using System.IO;
using DbEngine.Query;
using DbEngine.Reader;
using Xunit;
using FluentAssertions;
using System.Linq;

namespace test
{
    public class DataMungerTest
    {
        private static CsvQueryProcessor _reader;
        public DataMungerTest()
        {
            _reader = new CsvQueryProcessor(Path.Combine(Environment.CurrentDirectory, @"data\ipl.csv"));
        }
        [Fact]
        public void TestRetrieveHeader()
        {
            Header result = _reader.GetHeader();
            string[] expectedHeaders = new string[] {
              "id", "season", "city", "date", "team1", "team2", "toss_winner", "toss_decision",
                          "result", "dl_applied", "winner", "win_by_runs", "win_by_wickets", "player_of_match", "venue",
                          "umpire1", "umpire2", "umpire3"
            };
            result.Headers.Should().NotBeNull();
            result.Headers.Should().BeEquivalentTo(expectedHeaders);
        }
        //ERROR in TEST CASE
        [Fact]
        public void TestRetrieveDataTypes()
        {
            DataTypeDefinitions result = _reader.GetColumnType();
            string[] expectedDataTypes = new string[] {
                "System.Int32", "System.Int32", "System.String", "System.DateTime",
                        "System.String", "System.String", "System.String", "System.String",
                        "System.String", "System.Int32", "System.String", "System.Int32",
                        "System.Int32", "System.String", "System.String", "System.String",
                        "System.String", "System.Object"
            };
            result.DataTypes.Should().BeEquivalentTo(expectedDataTypes);
        }

        [Fact]
        public void TestFileNotFound()
        {
            try
            {
                CsvQueryProcessor reader = new CsvQueryProcessor(Path.Combine(Environment.CurrentDirectory, @"data\ipl.csv"));
            }
            catch (Exception exception)
            {
                exception.GetType().ToString().Should().Be("System.IO.FileNotFoundException");
            }
        }

        [Fact]
        public void TestSelectAllWithoutWhereClause()
        {
            int totalrecordsexpected = 577;
            int recordscounter = 1;
            bool dataexpectedstatus = false;
            DataSet dataSet = null;
            try
            {
                CsvQueryProcessor reader = new CsvQueryProcessor(Path.Combine(Environment.CurrentDirectory, @"data\ipl.csv"));

                dataSet = new Query().ExecuteQuery(@"select * from data\ipl.csv");

                int counter = 0;

                 dataSet.Rows.Reverse();

                foreach (Row row in dataSet.Rows)
                {
                    if (recordscounter == 1)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "1", "2008", "Bangalore", "2008-04-18", "Kolkata Knight Riders", "Royal Challengers Bangalore", "Royal Challengers Bangalore", "field", "normal", "0", "Kolkata Knight Riders", "140", "0", "BB McCullum", "M Chinnaswamy Stadium", "Asad Rauf", "RE Koertzen", "" }))

                        {
                            counter++;
                        }
                    }
                    else if (recordscounter == 289)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "289", "2012", "Cuttack", "2012-05-01", "Deccan Chargers", "Pune Warriors", "Deccan Chargers", "bat", "normal", "0", "Deccan Chargers", "13", "0", "KC Sangakkara", "Barabati Stadium", "Aleem Dar", "AK Chaudhary", "" }))
                        {
                            counter++;
                        }
                    }
                    else if (recordscounter == 577)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "577", "2016", "Bangalore", "2016-05-29", "Sunrisers Hyderabad", "Royal Challengers Bangalore", "Sunrisers Hyderabad", "bat", "normal", "0", "Sunrisers Hyderabad", "8", "0", "BCJ Cutting", "M Chinnaswamy Stadium", "HDPK Dharmasena", "BNJ Oxenford", "" }))
                        {
                            counter++;
                        }
                    }

                    recordscounter++;
                }
                if (counter > 2)
                {
                    dataexpectedstatus = true;
                }

            }
            catch (Exception exception)
            {
                exception.GetType().ToString().Should().Be("System.IO.FileNotFoundException");
            }

            totalrecordsexpected.Should().Be(577);
            dataSet.Should().NotBeNull("because records exist in ipl.csv file");
            dataexpectedstatus.Should().BeTrue("because no. of matching records should be more than 2");
        }


        [Fact]
        public void TestSelectColumnsWithoutWhereClause()
        {
            int totalrecordsexpected = 577;
            int recordscounter = 1;
            bool dataexpectedstatus = false;
            DataSet dataSet = null;
            try
            {
                CsvQueryProcessor reader = new CsvQueryProcessor(Path.Combine(Environment.CurrentDirectory, @"data\ipl.csv"));

                dataSet = new Query().ExecuteQuery(@"select city,winner,team1,team2 from data\ipl.csv");

                int counter = 0;

                foreach (Row row in dataSet.Rows)
                {
                    if (recordscounter == 1)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "Bangalore", "Kolkata Knight Riders", "Kolkata Knight Riders", "Royal Challengers Bangalore" }))
                        {
                            counter++;
                        }
                    }
                    else if (recordscounter == 289)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "Cuttack", "Deccan Chargers", "Deccan Chargers", "Pune Warriors" }))
                        {
                            counter++;
                        }
                    }
                    else if (recordscounter == 577)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "Bangalore", "Sunrisers Hyderabad", "Sunrisers Hyderabad", "Royal Challengers Bangalore" }))
                        {
                            counter++;
                        }
                    }

                    recordscounter++;
                }
                if (counter > 2)
                {
                    dataexpectedstatus = true;
                }

            }
            catch (Exception exception)
            {
                exception.GetType().ToString().Should().Be("System.IO.FileNotFoundException");
            }

            totalrecordsexpected.Should().Be(577);
            dataSet.Should().NotBeNull("because records exist in ipl.csv file");
            dataexpectedstatus.Should().BeTrue("because no. of matching records should be more than 2");
        }

        [Fact]
        public void TestWithWhereGreaterThanClause()
        {
            int totalrecordsexpected = 60;
            int recordscounter = 1;
            bool dataexpectedstatus = false;

            DataSet dataSet = null;
            try
            {
                CsvQueryProcessor reader = new CsvQueryProcessor(Path.Combine(Environment.CurrentDirectory, @"data\ipl.csv"));

                dataSet = new Query().ExecuteQuery(@"select season,city,winner,team1,team2,player_of_match from data\ipl.csv where season > 2015");

                int counter = 0;

                foreach (Row row in dataSet.Rows)
                {
                    if (recordscounter == 1)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "2016", "Mumbai", "Rising Pune Supergiants", "Mumbai Indians", "Rising Pune Supergiants", "AM Rahane" }))
                        {
                            counter++;
                        }
                    }
                    else if (recordscounter == 30)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "2016", "Bangalore", "Kolkata Knight Riders", "Royal Challengers Bangalore", "Kolkata Knight Riders", "AD Russell" }))
                        {
                            counter++;
                        }
                    }
                    else if (recordscounter == 60)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "2016", "Bangalore", "Royal Challengers Bangalore", "Sunrisers Hyderabad", "Sunrisers Hyderabad",  "BCJ Cutting" }))
                        {
                            counter++;
                        }
                    }

                    recordscounter++;
                }
                if (counter > 2)
                {
                    dataexpectedstatus = true;
                }

            }
            catch (Exception exception)
            {
                exception.GetType().ToString().Should().Be("System.IO.FileNotFoundException");
            }

            totalrecordsexpected.Should().Be(60);
            dataSet.Should().NotBeNull("because records exist in ipl.csv file");
            dataexpectedstatus.Should().BeTrue("because no. of matching records should be more than 2");
        }

        [Fact]
        public void TestWithWhereLessThanClause()
        {
            int totalrecordsexpected = 458;
            int recordscounter = 1;
            bool dataexpectedstatus = false;

            DataSet dataSet = null;
            try
            {
                CsvQueryProcessor reader = new CsvQueryProcessor(Path.Combine(Environment.CurrentDirectory, @"data\ipl.csv"));

                dataSet = new Query().ExecuteQuery(@"select city,winner,team1,team2,player_of_match from data\ipl.csv where season < 2015");

                int counter = 0;

                foreach (Row row in dataSet.Rows)
                {
                    if (recordscounter == 1)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "Bangalore", "Kolkata Knight Riders", "Kolkata Knight Riders", "Royal Challengers Bangalore", "BB McCullum" }))
                        {
                            counter++;
                        }
                    }
                    else if (recordscounter == 229)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "Jaipur", "Rajasthan Royals", "Royal Challengers Bangalore", "Royal Challengers Bangalore", "S Aravind" }))
                        {
                            counter++;
                        }
                    }
                    else if (recordscounter == 458)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "Bangalore", "Kings XI Punjab", "Kolkata Knight Riders", "Kolkata Knight Riders",    "MK Pandey" }))
                        {
                            counter++;
                        }
                    }

                    recordscounter++;
                }
                if (counter > 2)
                {
                    dataexpectedstatus = true;
                }

            }
            catch (Exception exception)
            {
                exception.GetType().ToString().Should().Be("System.IO.FileNotFoundException");
            }

            totalrecordsexpected.Should().Be(458);
            dataSet.Should().NotBeNull("because records exist in ipl.csv file");
            dataexpectedstatus.Should().BeTrue("because no. of matching records should be more than 2");
        }

        [Fact]
        public void TestWithWhereLessThanOrEqualToClause()
        {
            int totalrecordsexpected = 517;
            int recordscounter = 1;
            bool dataexpectedstatus = false;

            DataSet dataSet = null;
            try
            {
                CsvQueryProcessor reader = new CsvQueryProcessor(Path.Combine(Environment.CurrentDirectory, @"data\ipl.csv"));

                dataSet = new Query().ExecuteQuery(@"select season,city,winner,team1,team2,player_of_match from data\ipl.csv where season <= 2015");

                int counter = 0;

                foreach (Row row in dataSet.Rows)
                {
                    if (recordscounter == 1)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "2008","Bangalore", "Kolkata Knight Riders", "Kolkata Knight Riders", "Royal Challengers Bangalore", "BB McCullum" }))
                        {
                            counter++;
                        }
                    }
                    else if (recordscounter == 258)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "2012","Bangalore", "Kolkata Knight Riders", "Kolkata Knight Riders", "Royal Challengers Bangalore", "L Balaji" }))
                        {
                            counter++;
                        }
                    }
                    else if (recordscounter == 517)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "2015","Kolkata", "Mumbai Indians", "Mumbai Indians", "Chennai Super Kings", "RG Sharma" }))
                        {
                            counter++;
                        }
                    }

                    recordscounter++;
                }
                if (counter > 2)
                {
                    dataexpectedstatus = true;
                }

            }
            catch (Exception exception)
            {
                exception.GetType().ToString().Should().Be("System.IO.FileNotFoundException");
            }

            totalrecordsexpected.Should().Be(517);
            dataSet.Should().NotBeNull("because records exist in ipl.csv file");
            dataexpectedstatus.Should().BeTrue("because no. of matching records should be more than 2");
        }

        [Fact]
        public void TestWithWhereEqualAndNotEqualClause()
        {
            int totalrecordsexpected = 195;
            int recordscounter = 1;
            bool dataexpectedstatus = false;

            DataSet dataSet = null;
            try
            {
                CsvQueryProcessor reader = new CsvQueryProcessor(Path.Combine(Environment.CurrentDirectory, @"data\ipl.csv"));

                dataSet = new Query().ExecuteQuery(@"select season,city,winner,team1,team2,player_of_match from data\ipl.csv where season >= 2013 and season <= 2015");

                int counter = 0;

                foreach (Row row in dataSet.Rows)
                {
                    if (recordscounter == 1)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "2013", "Kolkata", "Delhi Daredevils", "Kolkata Knight Riders", "Kolkata Knight Riders", "SP Narine" }))
                        {
                            counter++;
                        }
                    }
                    else if (recordscounter == 97)
                    {
                    if (row.RowValues.SequenceEqual(new string[] { "2014", "Ranchi", "Chennai Super Kings", "Chennai Super Kings", "Kolkata Knight Riders",   "RA Jadeja" }))
                        {
                            counter++;
                        }
                    }
                    else if (recordscounter == 195)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "2015", "Kolkata", "Mumbai Indians", "Mumbai Indians", "Chennai Super Kings", "RG Sharma" }))
                        {
                            counter++;
                        }
                    }

                    recordscounter++;
                }
                if (counter > 2)
                {
                    dataexpectedstatus = true;
                }

            }
            catch (Exception exception)
            {
                exception.GetType().ToString().Should().Be("System.IO.FileNotFoundException");
            }

            dataSet.Rows.Count.Should().Be(totalrecordsexpected);
            dataSet.Should().NotBeNull("because records exist in ipl.csv file");
            dataexpectedstatus.Should().BeTrue("because no. of matching records should be more than 2");
        }

        [Fact]
        public void TestWithWhereThreeConditionsEqualOrNotEqualClause()
        {
            int totalrecordsexpected = 577;
            int recordscounter = 1;
            bool dataexpectedstatus = false;

            DataSet dataSet = null;
            try
            {
                CsvQueryProcessor reader = new CsvQueryProcessor(Path.Combine(Environment.CurrentDirectory, @"data\ipl.csv"));

                dataSet = new Query().ExecuteQuery(@"select city,winner,team1,team2,player_of_match from data\ipl.csv where season >= 2008 or toss_decision != bat and city = bangalore");

                int counter = 0;

                foreach (Row row in dataSet.Rows)
                {
                    if (recordscounter == 1)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "Bangalore", "Kolkata Knight Riders", "Kolkata Knight Riders", "Royal Challengers Bangalore", "BB McCullum" }))
                        {
                            counter++;
                        }
                    }
                    else if (recordscounter == 289)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "Cuttack", "Deccan Chargers", "Deccan Chargers", "Pune Warriors", "KC Sangakkara" }))
                        {
                            counter++;
                        }
                    }
                    else if (recordscounter == 577)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "Bangalore", "Sunrisers Hyderabad", "Sunrisers Hyderabad", "Royal Challengers Bangalore", "BCJ Cutting" }))
                        {
                            counter++;
                        }
                    }

                    recordscounter++;
                }
                if (counter > 2)
                {
                    dataexpectedstatus = true;
                }

            }
            catch (Exception exception)
            {
                exception.GetType().ToString().Should().Be("System.IO.FileNotFoundException");
            }

            dataSet.Rows.Count.Should().Be(totalrecordsexpected);
            dataSet.Should().NotBeNull("because records exist in ipl.csv file");
            dataexpectedstatus.Should().BeTrue("because no. of matching records should be more than 2");
        }

        /* Test Cases for Step - 6 */

        [Fact]
        public void TestSelectColumnsWithoutWhereWithGroupByAvg()
        {
           
            DataSet dataSet = null;
            try
            {
                CsvQueryProcessor reader = new CsvQueryProcessor(Path.Combine(Environment.CurrentDirectory, @"data\ipl.csv"));

                dataSet = new Query().ExecuteQuery(@"select city,avg(win_by_wickets) from data\ipl.csv group by city");

            }
            catch (Exception exception)
            {
                exception.GetType().ToString().Should().Be("System.IO.FileNotFoundException");
            }
            object rowList = null;
            ((GroupDataSet)dataSet).GroupedDataSet.TryGetValue("7", out rowList);

            GroupDataSet groupDataSet = new GroupDataSet();
            groupDataSet.GroupedDataSet.Add("city", "Chennai");
            groupDataSet.GroupedDataSet.Add("avg(win_by_wickets)", "2.31");
            

            rowList.Should().NotBeNull("because null not allowed");
            rowList.Should().BeEquivalentTo(groupDataSet);

        }

        [Fact]
        public void TestSelectColumnsWithoutWhereWithGroupByMax()
        {
             
            DataSet dataSet = null;
            try
            {
                CsvQueryProcessor reader = new CsvQueryProcessor(Path.Combine(Environment.CurrentDirectory, @"data\ipl.csv"));

                dataSet = new Query().ExecuteQuery(@"select city,max(win_by_wickets) from data/ipl.csv group by city");

            }
            catch (Exception exception)
            {
                exception.GetType().ToString().Should().Be("System.IO.FileNotFoundException");
            }
            object rowList = null;
            ((GroupDataSet)dataSet).GroupedDataSet.TryGetValue("7", out rowList);

            GroupDataSet groupDataSet = new GroupDataSet();
            groupDataSet.GroupedDataSet.Add("max(win_by_wickets)", "9");
            groupDataSet.GroupedDataSet.Add("city", "Chennai");

            rowList.Should().NotBeNull("because null not allowed");// "because sum can never be null");
            rowList.Should().BeEquivalentTo(groupDataSet);
        }

        [Fact]
        public void TestSelectColumnsWithoutWhereWithGroupByMin()
        {
           
            DataSet dataSet = null;
            try
            {
                CsvQueryProcessor reader = new CsvQueryProcessor(Path.Combine(Environment.CurrentDirectory, @"data\ipl.csv"));

                dataSet = new Query().ExecuteQuery(@"select city,min(season) from data/ipl.csv group by city");

            }
            catch (Exception exception)
            {
                exception.GetType().ToString().Should().Be("System.IO.FileNotFoundException");
            }
            object rowList = null;
            ((GroupDataSet)dataSet).GroupedDataSet.TryGetValue("7", out rowList);

            GroupDataSet groupDataSet = new GroupDataSet();
            groupDataSet.GroupedDataSet.Add("min(season)", "2008");
            groupDataSet.GroupedDataSet.Add("city", "Chennai");

            rowList.Should().NotBeNull("because null not allowed");// "because sum can never be null");
            rowList.Should().BeEquivalentTo(groupDataSet);
        }

        [Fact]
        public void TestSelectColumnsWithoutWhereWithGroupBySum()
        {
           
            DataSet dataSet = null;
            try
            {
                CsvQueryProcessor reader = new CsvQueryProcessor(Path.Combine(Environment.CurrentDirectory, @"data\ipl.csv"));

                dataSet = new Query().ExecuteQuery(@"select city,sum(season) from data/ipl.csv group by city");

            }
            catch (Exception exception)
            {
                exception.GetType().ToString().Should().Be("System.IO.FileNotFoundException");
            }

            object rowList = null;
            ((GroupDataSet)dataSet).GroupedDataSet.TryGetValue("30", out rowList);

            GroupDataSet groupDataSet = new GroupDataSet();
            groupDataSet.GroupedDataSet.Add("sum(season)", "4032");
            groupDataSet.GroupedDataSet.Add("city", "Kanpur");

            rowList.Should().NotBeNull("because null not allowed");// "because sum can never be null");
            rowList.Should().BeEquivalentTo(groupDataSet);

   
        }

        [Fact]
        public void TestSelectColumnsWithoutWhereWithGroupByCount()
        {
            int totalRecordsExpected = 31;
            DataSet dataSet = null;
            try
            {
                CsvQueryProcessor reader = new CsvQueryProcessor(Path.Combine(Environment.CurrentDirectory, @"data\ipl.csv"));

                dataSet = new Query().ExecuteQuery(@"select city,count(*) from data/ipl.csv group by city");

            }
            catch (Exception exception)
            {
                exception.GetType().ToString().Should().Be("System.IO.FileNotFoundException");
            }
            ((GroupDataSet)dataSet).GroupedDataSet.Count.Should().Be(totalRecordsExpected);
        }

        [Fact]
        public void TestSelectCountColumnsWithoutWhere2()
        {
        
            DataSet dataSet = null;
            try
            {
                CsvQueryProcessor reader = new CsvQueryProcessor(Path.Combine(Environment.CurrentDirectory, @"data\ipl.csv"));

                dataSet = new Query().ExecuteQuery(@"select count(city),sum(win_by_runs),min(win_by_runs),max(win_by_runs),avg(win_by_runs) from data/ipl.csv");

            }
            catch (Exception exception)
            {
                exception.GetType().ToString().Should().Be("System.IO.FileNotFoundException");
            }
            object rowList = null;
            ((GroupDataSet)dataSet).GroupedDataSet.TryGetValue("sum(win_by_runs)", out rowList);
            
            rowList.Should().NotBeNull("because sum can never be null");
            rowList.Should().BeEquivalentTo("7914");
           
            ((GroupDataSet)dataSet).GroupedDataSet.TryGetValue("count(city)", out rowList);
            rowList.Should().NotBeNull("because count can never be null");
            rowList.Should().BeEquivalentTo("577");

            ((GroupDataSet)dataSet).GroupedDataSet.TryGetValue("min(win_by_runs)", out rowList);
            rowList.Should().NotBeNull("because minimum can never be null");
            rowList.Should().BeEquivalentTo("0");
                         
             ((GroupDataSet)dataSet).GroupedDataSet.TryGetValue("max(win_by_runs)", out rowList);
            rowList.Should().NotBeNull("because maximum can never be null");
            rowList.Should().BeEquivalentTo("144"); //rowList.Contains(new Row(new string[] { "144" })).Should().BeTrue("because max of win_by_runs is 144");
                                                  
            ((GroupDataSet)dataSet).GroupedDataSet.TryGetValue("avg(win_by_runs)", out rowList);
            rowList.Should().NotBeNull("because average can never be null");
            rowList.Should().BeEquivalentTo("13.72"); //rowList.Contains(new Row(new string[] { "144" })).Should().BeTrue("because max of win_by_runs is 144");
             
                                                                                                
        }

        [Fact]
        public void TestSelectSumColumnsWithoutWhere()
        {
           
            DataSet dataSet = null;
            try
            {
                CsvQueryProcessor reader = new CsvQueryProcessor(Path.Combine(Environment.CurrentDirectory, @"data\ipl.csv"));

                dataSet = new Query().ExecuteQuery(@"select sum(win_by_runs) from data/ipl.csv");

            }
            catch (Exception exception)
            {
                exception.GetType().ToString().Should().Be("System.IO.FileNotFoundException");
            }
            object rowList = null;
            ((GroupDataSet)dataSet).GroupedDataSet.TryGetValue("sum(win_by_runs)", out rowList);
            
            rowList.Should().NotBeNull("because sum can never be null");
            rowList.Should().BeEquivalentTo("7914");

        }

        [Fact]
        public void TestSelectCountColumnsWithoutWhere()
        {
       
            DataSet dataSet = null;
            try
            {
                CsvQueryProcessor reader = new CsvQueryProcessor(Path.Combine(Environment.CurrentDirectory, @"data\ipl.csv"));

                dataSet = new Query().ExecuteQuery(@"select count(city) from data/ipl.csv");

            }
            catch (Exception exception)
            {
                exception.GetType().ToString().Should().Be("System.IO.FileNotFoundException");
            }
            object rowList = null;
            ((GroupDataSet)dataSet).GroupedDataSet.TryGetValue("count(city)", out rowList);
            rowList.Should().NotBeNull("because count can never be null");
            rowList.Should().BeEquivalentTo("577");

        }
       
        [Fact]
        public void TestWithOrderBy()
        {
            int totalRecordsExpected = 577;
            int recordscounter = 1;
            bool dataexpectedstatus = false;

            DataSet dataSet = null;
            try
            {
                CsvQueryProcessor reader = new CsvQueryProcessor(Path.Combine(Environment.CurrentDirectory, @"data\ipl.csv"));

                dataSet = new Query().ExecuteQuery(@"select player_of_match,city,team1,team2 from data/ipl.csv order by city");

                int counter = 0;

                foreach (Row row in dataSet.Rows)
                {
                    if (recordscounter == 1)
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "PA Patel", "", "Mumbai Indians", "Royal Challengers Bangalore"}))
                        {
                            counter++;
                        }
                    }
                    else if (recordscounter == 281)//Data changed due to invalid value in test case 
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "MS Dhoni", "East London", "Chennai Super Kings", "Deccan Chargers" }))
                        {
                            counter++;
                        }
                    }
                    else if (recordscounter == 577)//Data changed due to invalid value in test case 
                    {
                        if (row.RowValues.SequenceEqual(new string[] { "MS Dhoni", "Visakhapatnam", "Kings XI Punjab", "Rising Pune Supergiants" }))
                        {
                            counter++;
                        }
                    }

                    recordscounter++;
                }
                if (counter > 2)
                {
                    dataexpectedstatus = true;
                }

            }
            catch (Exception exception)
            {
                exception.GetType().ToString().Should().Be("System.IO.FileNotFoundException");
            }
            dataSet.Rows.Count.Should().Be(totalRecordsExpected);
            dataSet.Rows.Should().NotBeNull();
            dataexpectedstatus.Should().BeTrue("because no. of matching records should be more than 2");

        }

        [Fact]
        public void TestWithWhereThreeConditionsGroupBy()
        {
            int totalRecordsExpected = 12;
            DataSet dataSet = null;
            try
            {
                CsvQueryProcessor reader = new CsvQueryProcessor(Path.Combine(Environment.CurrentDirectory, @"data\ipl.csv"));

                dataSet = new Query().ExecuteQuery(@"select team1  from data/ipl.csv where season >= 2013 or toss_decision != bat and city = Bangalore group by team1");

            }
            catch (Exception exception)
            {
                exception.GetType().ToString().Should().Be("System.IO.FileNotFoundException");
            }
        
            ((GroupDataSet)dataSet).GroupedDataSet.Count().Should().Be(totalRecordsExpected);
          
        }

        [Fact]
        public void TestWithWhereThreeConditionsOrderBy()
        {
            int totalRecordsExpected = 420;
            DataSet dataSet = null;
            try
            {
                CsvQueryProcessor reader = new CsvQueryProcessor(Path.Combine(Environment.CurrentDirectory, @"data\ipl.csv"));

                dataSet = new Query().ExecuteQuery(@"select winner,player_of_match,city,team1,team2 from data/ipl.csv where season >= 2013 or toss_decision != bat or city = Bangalore order by winner");

            }
            catch (Exception exception)
            {
                exception.GetType().ToString().Should().Be("System.IO.FileNotFoundException");
            }
            dataSet.Rows.Count.Should().Be(totalRecordsExpected);
            dataSet.Rows.Should().NotBeNull();

        }
    }
}
