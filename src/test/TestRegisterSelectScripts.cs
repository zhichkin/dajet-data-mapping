using DaJet.Data;
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
    [TestClass] public class TestRegisterSelectScripts
    {
        private readonly InfoBase InfoBase;
        private readonly IMetadataService MetadataService;

        private const string ПериодическийРегистрСведений = "РегистрСведений.ИсторияСтатусовЗаказовКлиентов";

        public TestRegisterSelectScripts()
        {
            MetadataService = new MetadataService()
                .UseDatabaseProvider(DatabaseProvider.SQLServer)
                .UseConnectionString("Data Source=zhichkin;Initial Catalog=cerberus;Integrated Security=True");
            
            InfoBase = MetadataService.OpenInfoBase();
        }
        private RecorderDataMapper GetRecorderDataMapper(string metadataName)
        {
            ApplicationObject metaObject = InfoBase.GetApplicationObjectByName(metadataName);

            RecorderDataMapper mapper = new RecorderDataMapper();
            mapper.Configure(new DataMapperOptions()
            {
                InfoBase = InfoBase,
                MetaObject = metaObject,
                ConnectionString = MetadataService.ConnectionString
            });

            return mapper;
        }
        private RegisterDataMapper GetRegisterDataMapper(string metadataName)
        {
            ApplicationObject metaObject = InfoBase.GetApplicationObjectByName(metadataName);

            RegisterDataMapper mapper = new RegisterDataMapper();
            mapper.Configure(new DataMapperOptions()
            {
                InfoBase = InfoBase,
                MetaObject = metaObject,
                ConnectionString = MetadataService.ConnectionString
            });

            return mapper;
        }
        
        [TestMethod] public void Script_Select_ПериодическийРегистрСведений()
        {
            RegisterDataMapper mapper = GetRegisterDataMapper(ПериодическийРегистрСведений);

            Console.WriteLine(mapper.BuildSelectStatementScript("t"));
        }
        [TestMethod] public void Script_CountNoFilter_ПериодическийРегистрСведений()
        {
            RegisterDataMapper mapper = GetRegisterDataMapper(ПериодическийРегистрСведений);

            Console.WriteLine($"Total count = {mapper.GetTotalRowCount()}");
            Console.WriteLine(mapper.BuildSelectCountScript());
        }
        [TestMethod] public void Script_CountWithFilter_ПериодическийРегистрСведений()
        {
            RegisterDataMapper mapper = GetRegisterDataMapper(ПериодическийРегистрСведений);

            List<FilterParameter> filter = new List<FilterParameter>()
            {
                new FilterParameter()
                {
                    Path = "Период",
                    Operator = ComparisonOperator.Equal,
                    Value = new DateTime(2021, 12, 1)
                }
            };

            mapper.Options.Filter = filter;

            Console.WriteLine($"Total count = {mapper.GetTotalRowCount()}");
            Console.WriteLine(mapper.BuildSelectCountScript());
        }
        [TestMethod] public void Script_CountWithBetweenFilter_ПериодическийРегистрСведений()
        {
            RegisterDataMapper mapper = GetRegisterDataMapper(ПериодическийРегистрСведений);

            List<FilterParameter> filter = new List<FilterParameter>()
            {
                new FilterParameter()
                {
                    Path = "Период",
                    Operator = ComparisonOperator.GreaterOrEqual,
                    Value = new DateTime(2021, 12, 1)
                },
                new FilterParameter()
                {
                    Path = "Период",
                    Operator = ComparisonOperator.Less,
                    Value = new DateTime(2021, 12, 3)
                }
            };

            mapper.Options.Filter = filter;

            Console.WriteLine($"Total count = {mapper.GetTotalRowCount()}");
            Console.WriteLine(mapper.BuildSelectCountScript());
        }
        [TestMethod] public void Script_PagingWithFilter_ПериодическийРегистрСведений()
        {
            RegisterDataMapper mapper = GetRegisterDataMapper(ПериодическийРегистрСведений);

            List<FilterParameter> filter = new List<FilterParameter>()
            {
                new FilterParameter()
                {
                    Path = "Период",
                    Operator = ComparisonOperator.Equal,
                    Value = new DateTime(2021, 12, 1)
                }
            };

            mapper.Options.Filter = filter;

            Console.WriteLine(mapper.BuildSelectPagingScript());
        }
        [TestMethod] public void Script_PagingWithBetweenFilter_ПериодическийРегистрСведений()
        {
            RegisterDataMapper mapper = GetRegisterDataMapper(ПериодическийРегистрСведений);

            List<FilterParameter> filter = new List<FilterParameter>()
            {
                new FilterParameter()
                {
                    Path = "Период",
                    Operator = ComparisonOperator.GreaterOrEqual,
                    Value = new DateTime(2021, 12, 1)
                },
                new FilterParameter()
                {
                    Path = "Период",
                    Operator = ComparisonOperator.Less,
                    Value = new DateTime(2021, 12, 2)
                }
            };

            mapper.Options.Filter = filter;

            Console.WriteLine(mapper.BuildSelectPagingScript());
        }

        [TestMethod] public void PagingNoFilter_Registers()
        {
            string[] names = new string[]
            {
                //"РегистрСведений.ОбычныйРегистрСведений",
                //"РегистрСведений.ПериодическийРегистрСведений"
                "РегистрСведений.ИсторияСтатусовЗаказовКлиентов"
            };

            foreach(string metadataName in names)
            {
                Console.WriteLine();
                Console.WriteLine($"{metadataName}");

                RegisterDataMapper mapper = GetRegisterDataMapper(metadataName);

                Console.WriteLine($"COUNT = {mapper.GetTotalRowCount()}");

                RegisterJsonSerializer serializer = new RegisterJsonSerializer(mapper);

                foreach (ReadOnlyMemory<byte> message in serializer.Serialize(100, 1))
                {
                    Console.WriteLine(Encoding.UTF8.GetString(message.Span));
                }
            }
        }
        [TestMethod] public void PagingNoFilter_Recorders()
        {
            string[] names = new string[]
            {
                "РегистрСведений.РегистрСведенийОдинРегистратор",
                "РегистрСведений.РегистраторПериодСекунда",
                "РегистрСведений.ПериодическийМногоРегистраторов",
                "РегистрНакопления.РегистрНакопленияОбороты",
                "РегистрНакопления.РегистрНакопленияОстатки"
            };

            foreach (string metadataName in names)
            {
                Console.WriteLine();
                Console.WriteLine($"{metadataName}");

                RecorderDataMapper mapper = GetRecorderDataMapper(metadataName);

                Console.WriteLine($"COUNT = {mapper.GetTotalRowCount()}");

                RecorderJsonSerializer serializer = new RecorderJsonSerializer(mapper);

                foreach (ReadOnlyMemory<byte> message in serializer.Serialize(100, 1))
                {
                    Console.WriteLine(Encoding.UTF8.GetString(message.Span));
                }
            }
        }
    }
}