using DbEngine.Query.Parser;
using System;
using System.Collections.Generic;
using System.Text;

namespace DbEngine.query.parser
{
    public class ConditionGroup
    {
        public List<Restriction> ConditionsGroup { get; set; }
        public string Operator { get; set; }
    }

}
