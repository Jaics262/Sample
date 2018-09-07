using System;

namespace DbEngine.Query.Parser
{
    /*
     This class is used for storing name of field, condition and value for 
     each conditions  and mention parameterized constructor
  */
    public class Restriction {
        string propertyName, propertyValue, condition;

        public Restriction(string propertyName, string propertyValue, string condition)
        {
            this.propertyName = propertyName;
            this.propertyValue = propertyValue;
            this.condition = condition;
        }

        public string GetPropertyValue
        {
            get
            {
                return propertyValue;
            }
        }

        public string GetCondition
        {
            get
            {
                return condition;
            }
        }

        public override bool Equals(object obj)
        {
            Restriction restriction = obj as Restriction;
            return restriction.propertyName == this.propertyName && restriction.propertyValue == this.propertyValue && restriction.condition == this.condition;
        }

        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
    }
}