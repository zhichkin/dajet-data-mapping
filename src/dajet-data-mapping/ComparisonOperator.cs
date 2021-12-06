using System.ComponentModel;

namespace DaJet.Data.Mapping
{
    public enum ComparisonOperator
    {
        [Description("=")] Equal,
        [Description("<>")] NotEqual,
        [Description("IN")] Contains,
        [Description(">")] Greater,
        [Description(">=")] GreaterOrEqual,
        [Description("<")] Less,
        [Description("<=")] LessOrEqual,
        [Description("BETWEEN")] Between
    }
}