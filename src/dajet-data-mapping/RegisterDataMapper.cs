using DaJet.Metadata.Model;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;

namespace DaJet.Data.Mapping
{
    public sealed class RegisterDataMapper : IDaJetDataMapper
    {
        private static readonly List<string> SystemPropertyOrder = new List<string>()
        {
            "Регистратор", // Recorder   - uuid
            "Период",      // Period     - DateTime
            "ВидДвижения", // RecordType - string { "Receipt", "Expense" }
            "Активность"   // Active     - bool
        };

        private string SELECT_COUNT_SCRIPT = string.Empty;
        private string SELECT_PAGING_SCRIPT = string.Empty;

        public RegisterDataMapper() { }
        public DataMapperOptions Options { get; private set; }
        public List<PropertyMapper> PropertyMappers { get; private set; } = new List<PropertyMapper>();
        public void Configure(DataMapperOptions options)
        {
            Options = options;
            ConfigureDataMapper();
        }
        public void Reconfigure()
        {
            SELECT_COUNT_SCRIPT = string.Empty;
            SELECT_PAGING_SCRIPT = string.Empty;
        }
        private void ConfigureDataMapper()
        {
            if (Options.MetaObject == null)
            {
                Options.MetaObject = Options.InfoBase.GetApplicationObjectByName(Options.MetadataName);
            }

            Options.IgnoreProperties = new List<string>()
            {
                "НомерСтроки"
            };

            OrderSystemProperties();
            ConfigurePropertyDataMappers();
        }
        private List<string> GetSystemPropertyOrder()
        {
            return SystemPropertyOrder;
        }
        private void OrderSystemProperties()
        {
            List<string> propertyOrder = GetSystemPropertyOrder();
            List<MetadataProperty> ordered = new List<MetadataProperty>();

            foreach (string propertyName in propertyOrder)
            {
                int p = 0;
                while (p < Options.MetaObject.Properties.Count)
                {
                    MetadataProperty property = Options.MetaObject.Properties[p];

                    if (property.Purpose == PropertyPurpose.System && property.Name == propertyName)
                    {
                        ordered.Add(property);
                        Options.MetaObject.Properties.RemoveAt(p);
                    }
                    else
                    {
                        p++; // take next property
                    }
                }
            }

            if (ordered.Count > 0)
            {
                Options.MetaObject.Properties.InsertRange(0, ordered);
            }
        }
        private void ConfigurePropertyDataMappers()
        {
            PropertyMappers.Clear();

            int ordinal = -1;

            for (int i = 0; i < Options.MetaObject.Properties.Count; i++)
            {
                MetadataProperty property = Options.MetaObject.Properties[i];

                if (Options.IgnoreProperties.Contains(property.Name))
                {
                    continue;
                }

                PropertyMapper mapper = new PropertyMapper(Options.InfoBase, Options.MetaObject, property);

                mapper.Initialize(ref ordinal);

                PropertyMappers.Add(mapper);
            }
        }

        public int GetTotalRowCount()
        {
            int rowCount = 0;

            using (SqlConnection connection = new SqlConnection(Options.ConnectionString))
            {
                connection.Open();

                using (SqlCommand command = connection.CreateCommand())
                {
                    command.CommandType = CommandType.Text;
                    command.CommandText = GetSelectCountScript();
                    command.CommandTimeout = Options.CommandTimeout;

                    ConfigureQueryParameters(command, Options.Filter);

                    rowCount = (int)command.ExecuteScalar();
                }
            }

            return rowCount;
        }
        public long TestGetPageDataRows(int size, int page)
        {
            Stopwatch watcher = new Stopwatch();

            watcher.Start();

            using (SqlConnection connection = new SqlConnection(Options.ConnectionString))
            {
                connection.Open();

                using (SqlCommand command = connection.CreateCommand())
                {
                    command.CommandType = CommandType.Text;
                    command.CommandText = GetSelectPagingScript();
                    command.CommandTimeout = Options.CommandTimeout; // seconds
                    command.Parameters.AddWithValue("PageSize", size);
                    command.Parameters.AddWithValue("PageNumber", page);

                    ConfigureQueryParameters(command, Options.Filter);

                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            // do nothing ¯\_(ツ)_/¯
                        }
                        reader.Close();
                    }
                }
            }

            watcher.Stop();

            return watcher.ElapsedMilliseconds;
        }
        public IEnumerable<IDataReader> GetPageDataRows(int size, int page)
        {
            using (SqlConnection connection = new SqlConnection(Options.ConnectionString))
            {
                connection.Open();

                using (SqlCommand command = connection.CreateCommand())
                {
                    command.CommandType = CommandType.Text;
                    command.CommandText = GetSelectPagingScript();
                    command.CommandTimeout = Options.CommandTimeout; // seconds
                    command.Parameters.AddWithValue("PageSize", size);
                    command.Parameters.AddWithValue("PageNumber", page);

                    ConfigureQueryParameters(command, Options.Filter);

                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            yield return reader;
                        }
                        reader.Close();
                    }
                }
            }
        }
        ///<summary>The method is correlated with <see cref="BuildWhereClause"/></summary>
        private void ConfigureQueryParameters(SqlCommand command, List<FilterParameter> filter)
        {
            if (Options.Filter == null || Options.Filter.Count == 0)
            {
                return;
            }

            for (int p = 0; p < Options.Filter.Count; p++)
            {
                FilterParameter parameter = Options.Filter[p];

                object value = parameter.Value;

                if (value is DateTime dateTime)
                {
                    value = dateTime.AddYears(Options.InfoBase.YearOffset);
                }

                command.Parameters.AddWithValue($"p{p}", value);
            }
        }
                
        public string GetSelectCountScript()
        {
            if (string.IsNullOrEmpty(SELECT_COUNT_SCRIPT))
            {
                SELECT_COUNT_SCRIPT = BuildSelectCountScript();
            }

            return SELECT_COUNT_SCRIPT;
        }
        public string GetSelectPagingScript()
        {
            if (string.IsNullOrEmpty(SELECT_PAGING_SCRIPT))
            {
                SELECT_PAGING_SCRIPT = BuildSelectPagingScript();
            }

            return SELECT_PAGING_SCRIPT;
        }

        public string BuildSelectCountScript()
        {
            StringBuilder script = new StringBuilder();

            script.Append($"SELECT COUNT(*) FROM {Options.MetaObject.TableName} WITH(NOLOCK)");

            if (Options.Filter != null && Options.Filter.Count > 0)
            {
                script.Append($" WHERE {BuildWhereClause(Options.Filter)}");
            }

            script.Append(";");

            return script.ToString();
        }
        public string BuildSelectStatementScript(string tableAlias)
        {
            StringBuilder script = new StringBuilder();

            script.Append("SELECT ");

            for (int i = 0; i < PropertyMappers.Count; i++)
            {
                PropertyMappers[i].BuildSelectCommand(script, tableAlias);
            }

            script.Remove(script.Length - 2, 2); // remove ", " from the end

            if (string.IsNullOrEmpty(tableAlias))
            {
                script.Append($" FROM {Options.MetaObject.TableName}");
            }
            else
            {
                script.Append($" FROM {Options.MetaObject.TableName} AS {tableAlias}");
            }

            return script.ToString();
        }


        public string BuildSelectPagingScript()
        {
            IndexInfo index = GetPagingIndex();

            StringBuilder script = new StringBuilder();

            script.Append("WITH cte AS ");
            script.Append($"(SELECT {BuildSelectClause(index)} ");
            script.Append($"FROM {Options.MetaObject.TableName} ");
            if (Options.Filter != null && Options.Filter.Count > 0)
            {
                script.Append($"WHERE {BuildWhereClause(Options.Filter)} ");
            }
            script.Append($"ORDER BY {BuildOrderByClause(index)} ");
            script.Append("OFFSET @PageSize * (@PageNumber - 1) ROWS ");
            script.Append("FETCH NEXT @PageSize ROWS ONLY) ");
            script.Append(BuildSelectStatementScript("t"));
            script.Append(" INNER JOIN cte ON ");
            script.Append(BuildJoinOnClause(index));
            script.Append(";");

            return script.ToString();
        }
        private string BuildSelectClause(IndexInfo index)
        {
            StringBuilder clause = new StringBuilder();

            foreach (IndexColumnInfo column in index.Columns)
            {
                if (clause.Length > 0)
                {
                    clause.Append(", ");
                }
                clause.Append($"{column.Name}");
            }

            return clause.ToString();
        }
        private string BuildWhereClause(List<FilterParameter> filter)
        {
            if (filter != null && filter.Count == 0)
            {
                return string.Empty;
            }

            StringBuilder clause = new StringBuilder();

            for (int p = 0; p < filter.Count; p++)
            {
                FilterParameter parameter = filter[p];

                string fieldName = GetDatabaseFieldByPath(parameter.Path);
                string _operator = GetComparisonOperatorSymbol(parameter.Operator);

                if (clause.Length > 0)
                {
                    clause.Append(" AND ");
                }

                clause.Append($"{fieldName} {_operator} @p{p}");
            }

            return clause.ToString();
        }
        private string GetDatabaseFieldByPath(string path)
        {
            // TODO: multi-part path (example: Регистратор.Дата)
            foreach (MetadataProperty property in Options.MetaObject.Properties)
            {
                if (property.Name == path)
                {
                    if (property.Fields.Count > 0)
                    {
                        return property.Fields[0].Name;
                    }
                }
            }
            return string.Empty;
        }
        private string GetComparisonOperatorSymbol(ComparisonOperator comparisonOperator)
        {
            if (comparisonOperator == ComparisonOperator.Equal) return "=";
            else if (comparisonOperator == ComparisonOperator.NotEqual) return "<>";
            else if (comparisonOperator == ComparisonOperator.Less) return "<";
            else if (comparisonOperator == ComparisonOperator.LessOrEqual) return "<=";
            else if (comparisonOperator == ComparisonOperator.Greater) return ">";
            else if (comparisonOperator == ComparisonOperator.GreaterOrEqual) return ">=";
            else if (comparisonOperator == ComparisonOperator.Contains) return "IN";
            else if (comparisonOperator == ComparisonOperator.Between) return "BETWEEN";

            throw new ArgumentOutOfRangeException(nameof(comparisonOperator));
        }
        private string BuildOrderByClause(IndexInfo index)
        {
            StringBuilder clause = new StringBuilder();

            foreach (IndexColumnInfo column in index.Columns)
            {
                if (clause.Length > 0)
                {
                    clause.Append(", ");
                }
                clause.Append($"{column.Name} {(column.IsDescending ? "DESC" : "ASC")}");
            }

            return clause.ToString();
        }
        private string BuildJoinOnClause(IndexInfo index)
        {
            StringBuilder clause = new StringBuilder();

            foreach (IndexColumnInfo column in index.Columns)
            {
                if (clause.Length > 0)
                {
                    clause.Append(" AND ");
                }
                clause.Append($"t.{column.Name} = cte.{column.Name}");
            }

            return clause.ToString();
        }



        public IndexInfo GetPagingIndex()
        {
            if (Options.MetaObject is InformationRegister register)
            {
                if (register.UseRecorder) // Подчинение регистратору
                {
                    if (register.Periodicity == RegisterPeriodicity.None)
                    {
                        return GetPagingIndexForRecorderNonPeriodicRegister(); // Непериодический
                    }
                    else if (register.Periodicity == RegisterPeriodicity.Year
                        || register.Periodicity == RegisterPeriodicity.Quarter
                        || register.Periodicity == RegisterPeriodicity.Month
                        || register.Periodicity == RegisterPeriodicity.Day
                        || register.Periodicity == RegisterPeriodicity.Second)
                    {
                        return GetPagingIndexForRecorderNonPeriodicRegister(); // Периодический
                    }
                    else // RegisterPeriodicity.Recorder
                    {
                        return GetPagingIndexForRecorderRegister(); // Периодический по позиции регистратора
                    }
                }
                else if (register.Periodicity == RegisterPeriodicity.None)
                {
                    return GetPagingIndexForIndependentRegister(); // Независимый и Непериодический
                }
                else
                {
                    return GetPagingIndexForPeriodicRegister(); // Независимый и Периодический
                }
            }
            else if (Options.MetaObject is AccumulationRegister)
            {
                GetPagingIndexForRecorderRegister();
            }

            return GetClusteredIndex();
        }
        private IndexInfo GetClusteredIndex()
        {
            List<IndexInfo> indexes = SQLHelper.GetIndexes(Options.ConnectionString, Options.MetaObject.TableName);

            IndexInfo clustered = indexes.Where(i => i.IsClustered).FirstOrDefault();

            return clustered;
        }
        private IndexInfo GetPagingIndexForPeriodicRegister()
        {
            List<IndexInfo> indexes = SQLHelper.GetIndexes(Options.ConnectionString, Options.MetaObject.TableName);

            List<MetadataProperty> template = GetSelectionProperties();

            return GetUniqueIndexByTemplate(indexes, template);
        }
        private IndexInfo GetPagingIndexForIndependentRegister()
        {
            List<IndexInfo> indexes = SQLHelper.GetIndexes(Options.ConnectionString, Options.MetaObject.TableName);

            List<MetadataProperty> template = GetSelectionProperties();

            return GetUniqueIndexByTemplate(indexes, template);
        }
        private IndexInfo GetPagingIndexForRecorderRegister()
        {
            List<IndexInfo> indexes = SQLHelper.GetIndexes(Options.ConnectionString, Options.MetaObject.TableName);

            List<MetadataProperty> template = new List<MetadataProperty>()
            {
                GetPeriodProperty(),
                GetRecorderProperty(),
                GetRowNumberProperty()
            };

            return GetUniqueIndexByTemplate(indexes, template);
        }
        private IndexInfo GetPagingIndexForRecorderNonPeriodicRegister()
        {
            List<IndexInfo> indexes = SQLHelper.GetIndexes(Options.ConnectionString, Options.MetaObject.TableName);

            List<MetadataProperty> template = new List<MetadataProperty>()
            {
                GetRecorderProperty(),
                GetRowNumberProperty()
            };

            return GetUniqueIndexByTemplate(indexes, template);
        }

        private MetadataProperty GetPeriodProperty()
        {
            return Options.MetaObject.Properties.Where(p => p.Name == "Период").FirstOrDefault();
        }
        private MetadataProperty GetRecorderProperty()
        {
            return Options.MetaObject.Properties.Where(p => p.Name == "Регистратор").FirstOrDefault();
        }
        private MetadataProperty GetRowNumberProperty()
        {
            return Options.MetaObject.Properties.Where(p => p.Name == "НомерСтроки").FirstOrDefault();
        }
        private List<MetadataProperty> GetDimensions()
        {
            return Options.MetaObject.Properties.Where(p => p.Purpose == PropertyPurpose.Dimension).ToList();
        }
        private List<MetadataProperty> GetIndexProperties(IndexInfo index)
        {
            List<MetadataProperty> list = new List<MetadataProperty>();

            foreach (IndexColumnInfo column in index.Columns)
            {
                MetadataProperty property = GetPropertyByIndexColumn(column);

                if (property == null)
                {
                    continue;
                }

                if (!list.Contains(property))
                {
                    list.Add(property);
                }
            }

            return list;
        }
        private MetadataProperty GetPropertyByIndexColumn(IndexColumnInfo column)
        {
            foreach (MetadataProperty property in Options.MetaObject.Properties)
            {
                foreach (DatabaseField field in property.Fields)
                {
                    if (field.Name == column.Name)
                    {
                        return property;
                    }
                }
            }
            return null;
        }
        private IndexInfo GetUniqueIndexByTemplate(List<IndexInfo> indexes, List<MetadataProperty> template)
        {
            foreach (IndexInfo index in indexes)
            {
                if (!index.IsUnique)
                {
                    continue;
                }

                List<MetadataProperty> indexProperties = GetIndexProperties(index);

                if (template.Count != indexProperties.Count)
                {
                    continue;
                }

                int count = 0;

                for (int p = 0; p < template.Count; p++)
                {
                    if (template[p] == indexProperties[p])
                    {
                        count++;
                    }
                }

                if (template.Count == count)
                {
                    return index;
                }
            }

            return null;
        }


        public List<MetadataProperty> GetSelectionProperties()
        {
            if (Options.MetaObject is InformationRegister register)
            {
                if (register.UseRecorder) // Подчинение регистратору
                {
                    return GetRecorderRegisterSelectionProperties(); // Непериодический | Периодический | Периодический по позиции регистратора
                }
                else if (register.Periodicity == RegisterPeriodicity.None)
                {
                    return GetDimensions(); // Независимый и Непериодический
                }
                else
                {
                    return GetPeriodicRegisterSelectionProperties(); // Независимый и Периодический
                }
            }
            else if (Options.MetaObject is AccumulationRegister)
            {
                return GetRecorderRegisterSelectionProperties();
            }
            return null;
        }
        private List<MetadataProperty> GetRecorderRegisterSelectionProperties()
        {
            return new List<MetadataProperty>()
            {
                GetRecorderProperty()
            };
        }
        private List<MetadataProperty> GetPeriodicRegisterSelectionProperties()
        {
            MetadataProperty period = GetPeriodProperty();
            
            List<MetadataProperty> selection = GetDimensions();

            selection.Insert(0, period);
            
            return selection;
        }
    }
}