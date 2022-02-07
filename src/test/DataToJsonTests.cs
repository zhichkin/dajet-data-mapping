using DaJet.Data.Mapping;
using DaJet.Json;
using DaJet.Metadata;
using DaJet.Metadata.Model;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Text;

namespace test
{
    [TestClass] public class DataToJsonTests
    {
        private readonly IMetadataService MetadataService;
        public DataToJsonTests()
        {
            MetadataService = new MetadataService()
                .UseDatabaseProvider(DatabaseProvider.SQLServer)
                .UseConnectionString("Data Source=zhichkin;Initial Catalog=cerberus;Integrated Security=True");
        }

        [TestMethod] public void CatalogToJson()
        {
            string metadataName = "Справочник.Сотрудники";//"Справочник.СтавкиНДС"; //"Справочник.Клиенты"; // "Справочник.Файлы";

            InfoBase infoBase = MetadataService.OpenInfoBase();

            EntityDataMapper mapper = new EntityDataMapper();
            mapper.Configure(new DataMapperOptions()
            {
                InfoBase = infoBase,
                MetadataName = metadataName,
                ConnectionString = MetadataService.ConnectionString
            });

            EntityJsonSerializer serializer = new EntityJsonSerializer(mapper);

            int pageSize = 10;
            int pageNumber = 1;

            Console.WriteLine($"Rows count = {mapper.GetTotalRowCount()}");

            foreach (ReadOnlyMemory<byte> bytes in serializer.Serialize(pageSize, pageNumber))
            {
                Console.WriteLine(Encoding.UTF8.GetString(bytes.Span));
            }
        }
        [TestMethod] public void DocumentToJson()
        {
            string metadataName = "Документ.ЗаказКлиента";

            InfoBase infoBase = MetadataService.OpenInfoBase();

            EntityDataMapper mapper = new EntityDataMapper();
            mapper.Configure(new DataMapperOptions()
            {
                InfoBase = infoBase,
                MetadataName = metadataName,
                ConnectionString = MetadataService.ConnectionString
            });

            EntityJsonSerializer serializer = new EntityJsonSerializer(mapper);

            int pageSize = 10;
            int pageNumber = 1;

            Console.WriteLine($"Rows count = {mapper.GetTotalRowCount()}");

            foreach (ReadOnlyMemory<byte> bytes in serializer.Serialize(pageSize, pageNumber))
            {
                Console.WriteLine(Encoding.UTF8.GetString(bytes.Span));
            }
        }

        [TestMethod] public void Справочник_Предопределённые()
        {
            string metadataName = "Справочник.СправочникПредопределённые";
            TestJsonSerializer(metadataName);
        }
        [TestMethod] public void Справочник_Подчинённый()
        {
            string metadataName = "Справочник.СправочникПодчинённый";
            TestJsonSerializer(metadataName);
        }
        [TestMethod] public void Справочник_ПодчинённыйСоставной()
        {
            string metadataName = "Справочник.СправочникПодчинённыйСоставной";
            TestJsonSerializer(metadataName);
        }
        [TestMethod] public void Справочник_ИерархическийГруппы()
        {
            string metadataName = "Справочник.СправочникИерархическийГруппы";
            TestJsonSerializer(metadataName);
        }
        [TestMethod] public void Справочник_ИерархическийЭлементы()
        {
            string metadataName = "Справочник.СправочникИерархическийЭлементы";
            TestJsonSerializer(metadataName);
        }
        [TestMethod] public void Справочник_Тестовый()
        {
            string metadataName = "Справочник.ТестовыйСправочник";
            TestJsonSerializer(metadataName);
        }
        [TestMethod] public void Справочник_Номенклатура()
        {
            string metadataName = "Справочник.Номенклатура";
            TestJsonSerializer(metadataName);
        }
        [TestMethod] public void Документ_ЗаказКлиента()
        {
            string metadataName = "Документ.ЗаказКлиента";
            TestJsonSerializer(metadataName);
        }
        private void TestJsonSerializer(string metadataName)
        {
            if (!MetadataService.TryOpenInfoBase(out InfoBase infoBase, out string error))
            {
                Console.WriteLine($"Error: {error}");
                return;
            }

            EntityDataMapper mapper = new EntityDataMapper();
            mapper.Configure(new DataMapperOptions()
            {
                InfoBase = infoBase,
                MetadataName = metadataName,
                ConnectionString = MetadataService.ConnectionString
            });
            EntityJsonSerializer serializer = new EntityJsonSerializer(mapper);

            int pageSize = 15;
            int pageNumber = 1;

            Console.WriteLine($"Rows count = {mapper.GetTotalRowCount()}");

            foreach (ReadOnlyMemory<byte> bytes in serializer.Serialize(pageSize, pageNumber))
            {
                Console.WriteLine(Encoding.UTF8.GetString(bytes.Span));
            }
        }

        [TestMethod] public void MS_SelectEntityByUuidToJson()
        {
            string MS_CONNECTION_STRING = "Data Source=zhichkin;Initial Catalog=dajet-messaging-ms;Integrated Security=True";

            if (!new MetadataService()
                .UseDatabaseProvider(DatabaseProvider.SQLServer)
                .UseConnectionString(MS_CONNECTION_STRING)
                .TryOpenInfoBase(out InfoBase infoBase, out string error))
            {
                Console.WriteLine(error);
                return;
            }

            string metadataName = "Справочник.ВерсионируемыйСправочник";
            Guid entityUuid = new Guid("8d40c79c-935c-8ecc-11ec-84e038e27419");

            //EntityDataMapper mapper = new EntityDataMapper();
            //mapper.Configure(new DataMapperOptions()
            //{
            //    InfoBase = infoBase,
            //    MetadataName = "Справочник.ВерсионируемыйСправочник",
            //    ConnectionString = MS_CONNECTION_STRING
            //});
            //EntityJsonSerializer serializer = new EntityJsonSerializer(mapper);


            EntityDataMapperProvider provider = new EntityDataMapperProvider(infoBase, DatabaseProvider.SQLServer, MS_CONNECTION_STRING);

            EntityJsonSerializer serializer = new EntityJsonSerializer(provider);

            ReadOnlyMemory<byte> bytes = serializer.Serialize(metadataName, entityUuid);
            
            Console.WriteLine(Encoding.UTF8.GetString(bytes.Span));

            bytes = serializer.Serialize(metadataName, entityUuid);

            Console.WriteLine(Encoding.UTF8.GetString(bytes.Span));
        }
        [TestMethod] public void PG_SelectEntityByUuidToJson()
        {
            string PG_CONNECTION_STRING = "Host=127.0.0.1;Port=5432;Database=dajet-messaging-pg;Username=postgres;Password=postgres;";

            if (!new MetadataService()
                .UseDatabaseProvider(DatabaseProvider.PostgreSQL)
                .UseConnectionString(PG_CONNECTION_STRING)
                .TryOpenInfoBase(out InfoBase infoBase, out string error))
            {
                Console.WriteLine(error);
                return;
            }

            string metadataName = "Справочник.ВерсионируемыйСправочник";
            Guid entityUuid = new Guid("8d40c79c-935c-8ecc-11ec-84ec52b030cb");
            
            //EntityDataMapper mapper = new EntityDataMapper();
            //mapper.Configure(new DataMapperOptions()
            //{
            //    InfoBase = infoBase,
            //    MetadataName = "Справочник.ВерсионируемыйСправочник",
            //    ConnectionString = MS_CONNECTION_STRING
            //});
            //EntityJsonSerializer serializer = new EntityJsonSerializer(mapper);


            EntityDataMapperProvider provider = new EntityDataMapperProvider(infoBase, DatabaseProvider.PostgreSQL, PG_CONNECTION_STRING);

            EntityJsonSerializer serializer = new EntityJsonSerializer(provider);

            ReadOnlyMemory<byte> bytes = serializer.Serialize(metadataName, entityUuid);

            Console.WriteLine(Encoding.UTF8.GetString(bytes.Span));

            bytes = serializer.Serialize(metadataName, entityUuid);

            Console.WriteLine(Encoding.UTF8.GetString(bytes.Span));
        }
    }
}