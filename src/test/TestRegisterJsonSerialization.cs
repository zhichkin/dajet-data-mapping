using DaJet.Data.Mapping;
using DaJet.Json;
using DaJet.Metadata;
using DaJet.Metadata.Model;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Text;

namespace test
{
    [TestClass] public class TestRegisterJsonSerialization
    {
        private readonly InfoBase InfoBase;
        private readonly IMetadataService MetadataService;

        private const string ПериодическийРегистрСведений = "РегистрСведений.ПериодическийРегистрСведений";

        public TestRegisterJsonSerialization()
        {
            MetadataService = new MetadataService()
                .UseDatabaseProvider(DatabaseProvider.SQLServer)
                .UseConnectionString("Data Source=zhichkin;Initial Catalog=dajet-metadata;Integrated Security=True");

            InfoBase = MetadataService.OpenInfoBase();
        }
        private RegisterJsonSerializer GetSerializer(string metadataName)
        {
            ApplicationObject metaObject = InfoBase.GetApplicationObjectByName(metadataName);

            RegisterDataMapper mapper = new RegisterDataMapper()
                .Configure(new DataMapperOptions()
                {
                    InfoBase = InfoBase,
                    MetaObject = metaObject,
                    ConnectionString = MetadataService.ConnectionString
                });
            RegisterJsonSerializer serializer = new RegisterJsonSerializer(mapper);

            return serializer;
        }

        [TestMethod] public void Json_ПериодическийРегистрСведений()
        {
            RegisterJsonSerializer serializer = GetSerializer(ПериодическийРегистрСведений);
            serializer.DataMapper.Options.Filter = new List<FilterParameter>()
            {
                new FilterParameter()
                {
                    Path = "Период",
                    Operator = ComparisonOperator.Equal,
                    Value = new DateTime(2021, 12, 1)
                }
            };

            int pageSize = 100;
            int pageNumber = 1;
            foreach (ReadOnlyMemory<byte> bytes in serializer.Serialize(pageSize, pageNumber))
            {
                Console.WriteLine(Encoding.UTF8.GetString(bytes.Span));
            }
        }
    }
}