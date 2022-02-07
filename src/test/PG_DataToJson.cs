using DaJet.Data;
using DaJet.Data.Mapping;
using DaJet.Json;
using DaJet.Metadata;
using DaJet.Metadata.Model;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace test
{
    [TestClass] public class PG_DataToJson
    {
        private const string PG_CONNECTION_STRING = "Host=127.0.0.1;Port=5432;Database=dajet-metadata-pg;Username=postgres;Password=postgres;";
        
        private readonly IMetadataService MetadataService;
        public PG_DataToJson()
        {
            MetadataService = new MetadataService()
                .UseConnectionString(PG_CONNECTION_STRING)
                .UseDatabaseProvider(DatabaseProvider.PostgreSQL);
        }
        private List<Guid> GetReferences(string tableName)
        {
            List<Guid> list = new List<Guid>();

            using (NpgsqlConnection connection = new NpgsqlConnection(PG_CONNECTION_STRING))
            {
                connection.Open();

                using (NpgsqlCommand command = connection.CreateCommand())
                {
                    command.CommandType = CommandType.Text;
                    command.CommandText = "SELECT _idrref FROM " + tableName + ";";

                    using (NpgsqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            byte[] uuid = (byte[])reader.GetValue(0);

                            list.Add(new Guid(uuid));
                        }
                        reader.Close();
                    }
                }
            }

            return list;
        }
        private void TestJsonSerializer(string metadataName)
        {
            if (!MetadataService.TryOpenInfoBase(out InfoBase infoBase, out string error))
            {
                Console.WriteLine($"Error: {error}");
                return;
            }
            EntityDataMapperProvider provider = new EntityDataMapperProvider(infoBase, DatabaseProvider.PostgreSQL, PG_CONNECTION_STRING);

            EntityJsonSerializer serializer = new EntityJsonSerializer(provider);

            ApplicationObject entity = infoBase.GetApplicationObjectByName(metadataName);

            List<Guid> references = GetReferences(entity.TableName);

            foreach (Guid reference in references)
            {
                ReadOnlyMemory<byte> utf8 = serializer.Serialize(metadataName, reference);

                Console.WriteLine(Encoding.UTF8.GetString(utf8.Span));

                TestHelper.MsSendMessage("РегистрСведений.ВходящаяОчередь", utf8);
            }
        }

        [TestMethod] public void Справочник_БезКодаИНаименования()
        {
            string metadataName = "Справочник.СправочникБезКодаИНаименования";
            TestJsonSerializer(metadataName);
        }
        [TestMethod] public void Справочник_ПростойСправочник()
        {
            string metadataName = "Справочник.ПростойСправочник";
            TestJsonSerializer(metadataName);
        }
        [TestMethod] public void Справочник_Предопределённые()
        {
            string metadataName = "Справочник.СправочникПредопределённые";
            TestJsonSerializer(metadataName);
        }
        [TestMethod] public void Справочник_Владелец()
        {
            string metadataName = "Справочник.СправочникВладелец";
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
        [TestMethod] public void Документ_ЗаказКлиента()
        {
            string metadataName = "Документ.ЗаказКлиента";
            TestJsonSerializer(metadataName);
        }

        [TestMethod] public void УдалениеОбъектов()
        {
            if (!MetadataService.TryOpenInfoBase(out InfoBase infoBase, out string error))
            {
                Console.WriteLine($"Error: {error}");
                return;
            }

            EntityDataMapperProvider provider = new EntityDataMapperProvider(infoBase, DatabaseProvider.PostgreSQL, PG_CONNECTION_STRING);

            EntityJsonSerializer serializer = new EntityJsonSerializer(provider);

            string[] metadataNames = new string[]
            {
                "Справочник.ПростойСправочник",
                "Справочник.СправочникПредопределённые",
                "Справочник.СправочникВладелец",
                "Справочник.СправочникПодчинённый",
                "Справочник.СправочникПодчинённыйСоставной",
                "Справочник.СправочникИерархическийГруппы",
                "Справочник.СправочникИерархическийЭлементы",
                "Документ.ЗаказКлиента",
                "Справочник.СправочникБезКодаИНаименования"
            };

            foreach (string metadataName in metadataNames)
            {
                ApplicationObject entity = infoBase.GetApplicationObjectByName(metadataName);

                List<Guid> references = GetReferences(entity.TableName);

                foreach (Guid reference in references)
                {
                    Guid uuid = new Guid(SQLHelper.Get1CUuid(reference.ToByteArray()));

                    ReadOnlyMemory<byte> utf8 = serializer.SerializeAsObjectDeletion(metadataName, uuid);

                    Console.WriteLine(Encoding.UTF8.GetString(utf8.Span));

                    TestHelper.MsSendMessage("РегистрСведений.ВходящаяОчередь", utf8);
                }
            }
        }
    }
}