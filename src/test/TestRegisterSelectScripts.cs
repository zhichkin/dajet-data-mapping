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

        private const string ПериодическийРегистрСведений = "РегистрСведений.ПериодическийРегистрСведений";

        public TestRegisterSelectScripts()
        {
            MetadataService = new MetadataService()
                .UseDatabaseProvider(DatabaseProvider.SQLServer)
                .UseConnectionString("Data Source=zhichkin;Initial Catalog=dajet-metadata;Integrated Security=True");
            
            InfoBase = MetadataService.OpenInfoBase();
        }
        private RegisterDataMapper GetDataMapper(string metadataName)
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
            RegisterDataMapper mapper = GetDataMapper(ПериодическийРегистрСведений);

            Console.WriteLine(mapper.BuildSelectStatementScript("t"));
        }
        [TestMethod] public void Script_CountNoFilter_ПериодическийРегистрСведений()
        {
            RegisterDataMapper mapper = GetDataMapper(ПериодическийРегистрСведений);

            Console.WriteLine($"Total count = {mapper.GetTotalRowCount()}");
            Console.WriteLine(mapper.BuildSelectCountScript());
        }
        [TestMethod] public void Script_CountWithFilter_ПериодическийРегистрСведений()
        {
            RegisterDataMapper mapper = GetDataMapper(ПериодическийРегистрСведений);

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
            RegisterDataMapper mapper = GetDataMapper(ПериодическийРегистрСведений);

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
        [TestMethod] public void Script_PagingNoFilter_ПериодическийРегистрСведений()
        {
            RegisterDataMapper mapper = GetDataMapper(ПериодическийРегистрСведений);

            Console.WriteLine(mapper.BuildSelectPagingScript());
        }
        [TestMethod] public void Script_PagingWithFilter_ПериодическийРегистрСведений()
        {
            RegisterDataMapper mapper = GetDataMapper(ПериодическийРегистрСведений);

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
            RegisterDataMapper mapper = GetDataMapper(ПериодическийРегистрСведений);

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

        [TestMethod] public void Script_PagingNoFilter_РегистрСведений_РегистраторНепериодический()
        {
            RegisterDataMapper mapper = GetDataMapper("РегистрСведений.РегистрСведенийОдинРегистратор");

            Console.WriteLine(mapper.BuildSelectPagingScript());
        }
        [TestMethod] public void Script_PagingNoFilter_РегистрСведений_РегистраторПериодический()
        {
            RegisterDataMapper mapper = GetDataMapper("РегистрСведений.РегистраторПериодСекунда");

            Console.WriteLine(mapper.BuildSelectPagingScript());
        }
        [TestMethod] public void Script_PagingNoFilter_РегистрСведений_РегистраторПериодРегистратора()
        {
            RegisterDataMapper mapper = GetDataMapper("РегистрСведений.ПериодическийМногоРегистраторов");

            Console.WriteLine(mapper.BuildSelectPagingScript());
        }
        [TestMethod] public void Script_PagingNoFilter_РегистрНакопления_Обороты()
        {
            RegisterDataMapper mapper = GetDataMapper("РегистрНакопления.РегистрНакопленияОбороты");

            Console.WriteLine(mapper.BuildSelectPagingScript());
        }
        [TestMethod] public void Script_PagingNoFilter_РегистрНакопления_Остатки()
        {
            RegisterDataMapper mapper = GetDataMapper("РегистрНакопления.РегистрНакопленияОстатки");

            Console.WriteLine(mapper.BuildSelectPagingScript());
        }
    }
}