using DaJet.Metadata;
using DaJet.Metadata.Model;
using System.Collections.Generic;

namespace DaJet.Data.Mapping
{
    public sealed class DataMapperOptions
    {
        public InfoBase InfoBase { get; set; }
        public string MetadataName { get; set; }
        public DatabaseProvider Provider { get; set; }
        public string ConnectionString { get; set; }
        public int CommandTimeout { get; set; } = 60; // seconds
        public ApplicationObject MetaObject { get; set; }
        public IndexInfo Index { get; set; }
        public List<FilterParameter> Filter { get; set; }
        public List<string> IgnoreProperties { get; set; } = new List<string>();
        public List<EntityDataMapper> TablePartMappers { get; set; } = new List<EntityDataMapper>();
    }
}