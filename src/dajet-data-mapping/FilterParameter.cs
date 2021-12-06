namespace DaJet.Data.Mapping
{
    public sealed class FilterParameter
    {
        public string Path { get; set; }
        public ComparisonOperator Operator { get; set; }
        public object Value { get; set; }
    }
}