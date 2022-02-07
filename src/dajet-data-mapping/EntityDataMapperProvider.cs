using DaJet.Metadata;
using DaJet.Metadata.Model;
using System;
using System.Collections.Generic;

namespace DaJet.Data.Mapping
{
    public sealed class EntityDataMapperProvider
    {
        private readonly InfoBase _infoBase;
        private readonly DatabaseProvider _provider;
        private readonly string _connectionString;
        private readonly Dictionary<string, EntityDataMapper> _dataMappers = new Dictionary<string, EntityDataMapper>();
        public EntityDataMapperProvider(InfoBase infoBase, DatabaseProvider provider, string connectionString)
        {
            _infoBase = infoBase;
            _provider = provider;
            _connectionString = connectionString;
        }

        public bool TryGetDataMapper(string metadataName, out EntityDataMapper mapper)
        {
            if (_dataMappers.TryGetValue(metadataName, out mapper))
            {
                return true;
            }

            ApplicationObject entity = _infoBase.GetApplicationObjectByName(metadataName);

            if (!(entity is Catalog || entity is Document))
            {
                throw new ArgumentOutOfRangeException(nameof(metadataName));
            }

            EntityDataMapper item = new EntityDataMapper();
            item.Configure(new DataMapperOptions()
            {
                InfoBase = _infoBase,
                MetaObject = entity,
                Provider = _provider,
                ConnectionString = _connectionString
            });

            _dataMappers.Add(metadataName, item);

            return _dataMappers.TryGetValue(metadataName, out mapper);
        }
    }
}