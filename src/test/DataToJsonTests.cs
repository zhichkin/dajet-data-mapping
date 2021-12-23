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
                .UseConnectionString("Data Source=zhichkin;Initial Catalog=dajet-metadata;Integrated Security=True");
        }

        [TestMethod] public void CatalogToJson()
        {
            string metadataName = "Справочник.Номенклатура";//"Справочник.СтавкиНДС"; //"Справочник.Клиенты"; // "Справочник.Файлы";

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
    }
}