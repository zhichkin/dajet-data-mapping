using DaJet.Metadata.Model;
using System.Collections.Generic;

namespace DaJet.Data.Mapping
{
    public sealed class DataMapperOptions
    {
        public InfoBase InfoBase { get; set; }
        public string MetadataName { get; set; }
        public string ConnectionString { get; set; }
        public ApplicationObject MetaObject { get; set; }
        public List<string> IgnoreProperties { get; set; } = new List<string>();
        public List<EntityDataMapper> TablePartMappers { get; set; } = new List<EntityDataMapper>();
    }
}