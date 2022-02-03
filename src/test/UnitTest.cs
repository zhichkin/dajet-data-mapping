using DaJet.CodeGen;
using DaJet.Data;
using DaJet.Data.Mapping;
using DaJet.Json;
using DaJet.Metadata;
using DaJet.Metadata.Model;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

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

            if (typeName == "����������") collection = infoBase.Catalogs;
            else if (typeName == "��������") collection = infoBase.Documents;
            else if (typeName == "����������") collection = infoBase.Publications;
            else if (typeName == "���������������") collection = infoBase.InformationRegisters;
            else if (typeName == "�����������������") collection = infoBase.AccumulationRegisters;
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

            string metadataName = "���������������.����������������������";

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

            string metadataName = "���������������.����������������������";

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

            string metadataName = "���������������.����������������������";

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

            string metadataName = "���������������.����������������������";

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
            string metadataName = "����������.�������";

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
        

        [TestMethod] public void ShowIndexInfo()
        {
            string metadataName = "��������.������������";

            InfoBase infoBase = MetadataService.OpenInfoBase();
            ApplicationObject metaObject = infoBase.GetApplicationObjectByName(metadataName);

            Console.WriteLine($"Object: {metaObject.Name}");

            List<IndexInfo> indexes = SQLHelper.GetIndexes(
                MetadataService.ConnectionString,
                metaObject.TableName);

            foreach (IndexInfo index in indexes)
            {
                Console.WriteLine();
                Console.WriteLine($"Index: {index.Name}");
                Console.WriteLine($"Unique: {index.IsUnique}");
                Console.WriteLine($"Clustered: {index.IsClustered}");
                Console.WriteLine($"Primary key: {index.IsPrimaryKey}");

                foreach (IndexColumnInfo column in index.Columns)
                {
                    Console.WriteLine($"- No: {column.KeyOrdinal}");
                    Console.WriteLine($"- Name: {column.Name}");
                    Console.WriteLine($"- Type: {column.TypeName}");
                    Console.WriteLine($"- Nullable: {column.IsNullable}");
                    Console.WriteLine($"- Included: {column.IsIncluded}");
                    Console.WriteLine($"- Sort order: {(column.IsDescending ? "DESC" : "ASC")}");
                }
            }
        }

        [TestMethod] public void TestCatalogScriptsWithIndexAndFilter()
        {
            string metadataName = "����������.������";

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
            EntityJsonSerializer serializer = new EntityJsonSerializer(mapper);

            int pageSize = 1000;
            int pageNumber = 1;
            TestEntityDataMapper(mapper, pageSize, pageNumber);

            List<IndexInfo> indexes = SQLHelper.GetIndexes(MetadataService.ConnectionString, metaObject.TableName);
            IndexInfo clustered_index = indexes.Where(i => i.IsClustered).FirstOrDefault();
            IndexInfo bydescrip_index = indexes.Where(i => i.Name == "_Reference264_Descr").FirstOrDefault();

            mapper.Options.Index = clustered_index;
            mapper.Options.Filter = null;
            mapper.Reconfigure();
            TestEntityDataMapper(mapper, pageSize, pageNumber);

            List<FilterParameter> filter = new List<FilterParameter>()
            {
                new FilterParameter()
                {
                    Path = "������������",
                    Operator = ComparisonOperator.GreaterOrEqual,
                    Value = "������ 999990"
                }
            };
            mapper.Options.Index = bydescrip_index;
            mapper.Options.Filter = filter;
            mapper.Reconfigure();
            TestEntityDataMapper(mapper, pageSize, pageNumber);

            pageSize = 2;
            pageNumber = 5;
            foreach (ReadOnlyMemory<byte> message in serializer.Serialize(pageSize, pageNumber))
            {
                Console.WriteLine(Encoding.UTF8.GetString(message.Span));
            }

            filter = new List<FilterParameter>()
            {
                new FilterParameter()
                {
                    Path = "������������",
                    Operator = ComparisonOperator.GreaterOrEqual,
                    Value = "������ 999990"
                },
                new FilterParameter()
                {
                    Path = "������������",
                    Operator = ComparisonOperator.Less,
                    Value = "������ 999995"
                }
            };
            mapper.Options.Filter = filter;
            mapper.Reconfigure();
            TestEntityDataMapper(mapper, pageSize, pageNumber);

            pageSize = 10;
            pageNumber = 1;
            foreach (ReadOnlyMemory<byte> message in serializer.Serialize(pageSize, pageNumber))
            {
                Console.WriteLine(Encoding.UTF8.GetString(message.Span));
            }
        }
        [TestMethod] public void TestDocumentScriptsWithIndexAndFilter()
        {
            string metadataName = "��������.������������";

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

            int pageSize = 1000;
            int pageNumber = 1;
            TestEntityDataMapper(mapper, pageSize, pageNumber);

            List<IndexInfo> indexes = SQLHelper.GetIndexes(MetadataService.ConnectionString, metaObject.TableName);
            IndexInfo clustered_index = indexes.Where(i => i.IsClustered).FirstOrDefault();
            IndexInfo bydocdate_index = indexes.Where(i => i.Name == "_Document356_ByDocDate").FirstOrDefault();

            mapper.Options.Index = clustered_index;
            mapper.Options.Filter = null;
            mapper.Reconfigure();
            TestEntityDataMapper(mapper, pageSize, pageNumber);

            List<FilterParameter> filter = new List<FilterParameter>()
            {
                new FilterParameter()
                {
                    Path = "����",
                    Operator = ComparisonOperator.Greater,
                    Value = DateTime.Parse("2020-10-01T00:00:00")
                }
            };

            mapper.Options.Index = bydocdate_index;
            mapper.Options.Filter = filter;
            mapper.Reconfigure();
            TestEntityDataMapper(mapper, pageSize, pageNumber);
        }
        private void TestEntityDataMapper(EntityDataMapper mapper, int pageSize, int pageNumber)
        {
            Console.WriteLine($"Total row count = {mapper.GetTotalRowCount()}");
            Console.WriteLine();
            Console.WriteLine($"Test: {mapper.TestGetPageDataRows(pageSize, pageNumber)} ms");
            Console.WriteLine();
            Console.WriteLine(mapper.GetTotalRowCountScript());
            Console.WriteLine();
            Console.WriteLine(mapper.GetSelectEntityPagingScript());
            Console.WriteLine();
            Console.WriteLine(mapper.GetSelectTablePartScript());
            Console.WriteLine();
        }
    }
}