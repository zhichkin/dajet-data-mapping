using DaJet.Metadata.Model;
using Microsoft.Data.SqlClient;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace DaJet.Data.Mapping
{
    public sealed class RegisterDataMapper
    {
        private InfoBase InfoBase { get; }
        private ApplicationObject MetaObject { get; }
        private List<PropertyMapper> PropertyMappers { get; set; }
        private ClusteredIndexInfo IndexInfo { get; }

        private string SELECT_REGISTER_COUNT_SCRIPT = string.Empty;
        private string SELECT_REGISTER_PAGING_SCRIPT = string.Empty;

        public RegisterDataMapper(InfoBase infoBase, ApplicationObject metaObject, ClusteredIndexInfo indexInfo)
        {
            InfoBase = infoBase;
            MetaObject = metaObject;
            IndexInfo = indexInfo;
            InitializeDataMapper();
        }
        private void InitializeDataMapper()
        {
            PropertyMappers = ConfigurePropertyMappers(MetaObject);
        }
        private string GetRegisterPropertyName(MetadataProperty property)
        {
            if (property.Name == "Период") return "Period";
            else if (property.Name == "Регистратор") return "Recorder";

            return property.Name;
        }
        private List<PropertyMapper> ConfigurePropertyMappers(ApplicationObject metaObject)
        {
            List<PropertyMapper> mappers = new List<PropertyMapper>();

            int ordinal = -1;

            for (int i = 0; i < metaObject.Properties.Count; i++)
            {
                string propertyName = GetRegisterPropertyName(metaObject.Properties[i]);

                if (string.IsNullOrEmpty(propertyName))
                {
                    continue;
                }
                                
                PropertyMapper mapper = new PropertyMapper(InfoBase, metaObject.Properties[i]);
                mapper.Initialize(ref ordinal);
                mappers.Add(mapper);
            }

            return mappers;
        }

        public string GetTotalRowCountScript()
        {
            if (string.IsNullOrEmpty(SELECT_REGISTER_COUNT_SCRIPT))
            {
                StringBuilder script = new StringBuilder();

                script.Append($"SELECT COUNT(*) FROM {MetaObject.TableName} WITH(NOLOCK);");

                SELECT_REGISTER_COUNT_SCRIPT = script.ToString();
            }

            return SELECT_REGISTER_COUNT_SCRIPT;
        }
        public int GetTotalRowCount(string connectionString)
        {
            int rowCount = 0;

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();

                using (SqlCommand command = connection.CreateCommand())
                {
                    command.CommandType = CommandType.Text;
                    command.CommandText = GetTotalRowCountScript();
                    command.CommandTimeout = 60; // seconds

                    rowCount = (int)command.ExecuteScalar();
                }
            }

            return rowCount;
        }

        public string GetSelectRegisterPagingScript()
        {
            if (string.IsNullOrEmpty(SELECT_REGISTER_PAGING_SCRIPT))
            {
                List<DatabaseField> index = GetClusteredIndexFields();

                StringBuilder script = new StringBuilder();

                script.Append("WITH cte AS (SELECT ");
                
                foreach (DatabaseField field in index)
                {
                    script.Append($"{field.Name}, ");
                }
                script.Remove(script.Length - 2, 2); // remove ", " from the end

                script.Append($" FROM {MetaObject.TableName} ORDER BY ");

                foreach (DatabaseField field in index)
                {
                    script.Append($"{field.Name} ASC, ");
                }
                script.Remove(script.Length - 2, 2); // remove ", " from the end

                script.Append(" OFFSET @PageSize * (@PageNumber - 1) ROWS ");
                script.Append("FETCH NEXT @PageSize ROWS ONLY) ");
                script.Append(BuildSelectRegisterScript(MetaObject, PropertyMappers, "t"));
                script.Append(" INNER JOIN cte ON ");

                foreach (DatabaseField field in index)
                {
                    script.Append($"t.{field.Name} = cte.{field.Name} AND ");
                }
                script.Remove(script.Length - 5, 5); // remove " AND " from the end

                script.Append(";");

                SELECT_REGISTER_PAGING_SCRIPT = script.ToString();
            }

            return SELECT_REGISTER_PAGING_SCRIPT;
        }
        private List<DatabaseField> GetClusteredIndexFields()
        {
            List<DatabaseField> fields = new List<DatabaseField>();

            for (int i = 0; i < IndexInfo.COLUMNS.Count; i++)
            {
                ClusteredIndexColumnInfo column = IndexInfo.COLUMNS[i];

                foreach (MetadataProperty property in MetaObject.Properties)
                {
                    foreach (DatabaseField field in property.Fields)
                    {
                        if (field.Name == column.NAME)
                        {
                            fields.Add(field);
                        }
                    }
                }
            }

            return fields;
        }
        private string BuildSelectRegisterScript(ApplicationObject metaObject, List<PropertyMapper> mappers, string tableAlias)
        {
            StringBuilder script = new StringBuilder();

            script.Append("SELECT ");

            for (int i = 0; i < mappers.Count; i++)
            {
                mappers[i].BuildSelectCommand(script, tableAlias);
            }

            script.Remove(script.Length - 2, 2); // remove ", " from the end

            if (string.IsNullOrEmpty(tableAlias))
            {
                script.Append($" FROM {metaObject.TableName}");
            }
            else
            {
                script.Append($" FROM {metaObject.TableName} AS {tableAlias}");
            }

            return script.ToString();
        }
    }
}