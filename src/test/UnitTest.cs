using DaJet.CodeGen;
using DaJet.Data;
using DaJet.Data.Mapping;
using DaJet.Json;
using DaJet.Metadata;
using DaJet.Metadata.Model;
using DaJet.RabbitMQ;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;

namespace test
{
    [TestClass] public class UnitTest
    {
        private readonly IMetadataService MetadataService;
        public UnitTest()
        {
            MetadataService = new MetadataService()
                .UseDatabaseProvider(DatabaseProvider.SQLServer)
                .UseConnectionString("Data Source=zhichkin;Initial Catalog=cerberus;Integrated Security=True");
        }
        private ApplicationObject GetApplicationObjectByName(InfoBase infoBase, string metadataName)
        {
            string[] names = metadataName.Split('.');
            if (names.Length != 2)
            {
                return null;
            }

            string typeName = names[0];
            string objectName = names[1];

            ApplicationObject metaObject = null;
            Dictionary<Guid, ApplicationObject> collection = null;

            if (typeName == "—правочник") collection = infoBase.Catalogs;
            else if (typeName == "ƒокумент") collection = infoBase.Documents;
            else if (typeName == "ѕланќбмена") collection = infoBase.Publications;
            else if (typeName == "–егистр—ведений") collection = infoBase.InformationRegisters;
            else if (typeName == "–егистрЌакоплени€") collection = infoBase.AccumulationRegisters;
            if (collection == null)
            {
                return null;
            }

            metaObject = collection.Values.Where(o => o.Name == objectName).FirstOrDefault();
            if (metaObject == null)
            {
                return null;
            }

            return metaObject;
        }

        [TestMethod] public void GenerateEntityCode()
        {
            InfoBase infoBase = MetadataService.OpenInfoBase();

            string metadataName = "–егистр—ведений.ќтветственныеЋицајптек";

            ApplicationObject metaObject = GetApplicationObjectByName(infoBase, metadataName);
            if (metaObject == null)
            {
                Console.WriteLine($"Object \"{metadataName}\" is not found.");
                return;
            }

            EntityCodeGenerator generator = new EntityCodeGenerator();
            string sourceCode = generator.GenerateSourceCode(infoBase, metaObject);

            Console.WriteLine(sourceCode);
        }
        [TestMethod] public void GenerateSelectScript()
        {
            InfoBase infoBase = MetadataService.OpenInfoBase();

            string metadataName = "–егистр—ведений.ќтветственныеЋицајптек";

            ApplicationObject metaObject = GetApplicationObjectByName(infoBase, metadataName);
            if (metaObject == null)
            {
                Console.WriteLine($"Object \"{metadataName}\" is not found.");
                return;
            }

            EntityCodeGenerator generator = new EntityCodeGenerator();
            string scriptCode = generator.GenerateSelectScript(metaObject);

            //foreach (TablePart table in metaObject.TableParts)
            //{
            //    scriptCode = generator.GenerateSelectScript(table);
            //    Console.WriteLine(scriptCode);
            //}

            Console.WriteLine(scriptCode);
        }
        [TestMethod] public void GenerateDataMapperCode()
        {
            InfoBase infoBase = MetadataService.OpenInfoBase();

            string metadataName = "–егистр—ведений.ќтветственныеЋицајптек";

            ApplicationObject metaObject = GetApplicationObjectByName(infoBase, metadataName);
            if (metaObject == null)
            {
                Console.WriteLine($"Object \"{metadataName}\" is not found.");
                return;
            }

            EntityCodeGenerator generator = new EntityCodeGenerator();
            string mapperCode = generator.GenerateDataMapperCode(metaObject);

            //foreach (TablePart table in metaObject.TableParts)
            //{
            //    mapperCode = generator.GenerateDataMapperCode(table);
            //    Console.WriteLine(mapperCode);
            //}

            Console.WriteLine(mapperCode);
        }
        [TestMethod] public void ShowClusteredIndexInfo()
        {
            InfoBase infoBase = MetadataService.OpenInfoBase();

            string metadataName = "–егистр—ведений.ќтветственныеЋицајптек";

            ApplicationObject metaObject = GetApplicationObjectByName(infoBase, metadataName);
            if (metaObject == null)
            {
                Console.WriteLine($"Object \"{metadataName}\" is not found.");
                return;
            }

            Console.WriteLine($"Object: {metaObject.Name}");
            ShowTableClusteredIndexInfo(MetadataService.ConnectionString, metaObject.TableName);

            //foreach (TablePart table in metaObject.TableParts)
            //{
            //    Console.WriteLine($"Table part: {table.Name}");
            //    ShowTableClusteredIndexInfo(metadata.ConnectionString, table.TableName);
            //    Console.WriteLine();
            //}
        }
        private void ShowTableClusteredIndexInfo(string connectionString, string tableName)
        {
            ClusteredIndexInfo index = SQLHelper.GetClusteredIndexInfo(connectionString, tableName);

            Console.WriteLine($"Index: {index.NAME}");
            Console.WriteLine($"Unique: {index.IS_UNIQUE}");
            Console.WriteLine($"Primary key: {index.IS_PRIMARY_KEY}");
            foreach (ClusteredIndexColumnInfo column in index.COLUMNS)
            {
                Console.WriteLine($"- No: {column.KEY_ORDINAL}");
                Console.WriteLine($"- Name: {column.NAME}");
                Console.WriteLine($"- Nullable: {column.IS_NULLABLE}");
                Console.WriteLine($"- Sort order: {(column.IS_DESCENDING_KEY ? "DESC" : "ASC")}");
            }
        }
        [TestMethod] public void BuildSelectEntityScripts()
        {
            string metadataName = "—правочник. лиенты";

            InfoBase infoBase = MetadataService.OpenInfoBase();

            ApplicationObject metaObject = GetApplicationObjectByName(infoBase, metadataName);
            if (metaObject == null)
            {
                Console.WriteLine($"Object \"{metadataName}\" is not found.");
                return;
            }

            EntityDataMapper mapper = new EntityDataMapper();
            mapper.Configure(new DataMapperOptions()
            {
                InfoBase = infoBase,
                MetaObject = metaObject,
                ConnectionString = MetadataService.ConnectionString
            });

            Console.WriteLine(mapper.GetTotalRowCountScript());
            Console.WriteLine();
            Console.WriteLine(mapper.GetSelectEntityPagingScript());
            Console.WriteLine();
            Console.WriteLine(mapper.GetSelectTablePartScript());
        }
        [TestMethod] public void BuildSelectRegisterScripts()
        {
            string metadataName = "–егистр—ведений.ќтветственныеЋицајптек";

            InfoBase infoBase = MetadataService.OpenInfoBase();

            ApplicationObject metaObject = GetApplicationObjectByName(infoBase, metadataName);
            if (metaObject == null)
            {
                Console.WriteLine($"Object \"{metadataName}\" is not found.");
                return;
            }

            ClusteredIndexInfo indexInfo = SQLHelper.GetClusteredIndexInfo(MetadataService.ConnectionString, metaObject.TableName);

            RegisterDataMapper mapper = new RegisterDataMapper(infoBase, metaObject, indexInfo);

            Console.WriteLine(mapper.GetTotalRowCountScript());
            Console.WriteLine();
            Console.WriteLine(mapper.GetSelectRegisterPagingScript());
        }



        [TestMethod] public void PublishMessages()
        {
            InfoBase infoBase = MetadataService.OpenInfoBase();

            string metadataName = "ƒокумент.«аказ лиента";

            EntityDataMapper mapper = new EntityDataMapper()
                .Configure(new DataMapperOptions()
                {
                    InfoBase = infoBase,
                    MetadataName = metadataName,
                    ConnectionString = MetadataService.ConnectionString
                });
            EntityJsonSerializer serializer = new EntityJsonSerializer(mapper);

            string routingKey = "–»Ѕ.MAIN.N001"; // "—правочник. лиенты";
            string uri = "amqp://guest:guest@localhost:5672"; // /%2F /dajet-exchange

            int pageSize = 10;
            int pageNumber = 1;

            Console.WriteLine($"Rows count = {mapper.GetTotalRowCount()}");

            using (RabbitMQProducer producer = new RabbitMQProducer(uri, routingKey))
            {
                producer.Initialize();
                producer.AppId = "MAIN";
                producer.MessageType = metadataName;

                Console.WriteLine($"AppId: {producer.AppId}");
                Console.WriteLine($"Host: {producer.HostName}");
                Console.WriteLine($"Port: {producer.HostPort}");
                Console.WriteLine($"VHost: {producer.VirtualHost}");
                Console.WriteLine($"User: {producer.UserName}");
                Console.WriteLine($"Pass: {producer.Password}");
                Console.WriteLine($"Exchange: {producer.ExchangeName}");
                Console.WriteLine($"RoutingKey: {producer.RoutingKey}");
                Console.WriteLine($"MessageType: {producer.MessageType}");

                int messagesSent = producer.Publish(serializer, pageSize, pageNumber);

                Console.WriteLine($"Messages sent = {messagesSent}");
            }
        }
    }
}