using DaJet.Metadata;
using DaJet.Metadata.Model;
using Microsoft.Data.SqlClient;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace test
{
    public static class TestHelper
    {
        private const string MS_CONNECTION_STRING = "Data Source=zhichkin;Initial Catalog=dajet-metadata-ms;Integrated Security=True";
        private const string PG_CONNECTION_STRING = "Host=127.0.0.1;Port=5432;Database=dajet-metadata-pg;Username=postgres;Password=postgres;";

        public static void MsSendMessage(string metadataName, ReadOnlyMemory<byte> message)
        {
            if (!new MetadataService()
                .UseConnectionString(MS_CONNECTION_STRING)
                .UseDatabaseProvider(DatabaseProvider.SQLServer)
                .TryOpenInfoBase(out InfoBase infoBase, out string error))
            {
                throw new InvalidOperationException(error);
            }

            ApplicationObject queue = infoBase.GetApplicationObjectByName(metadataName);

            if (queue == null)
            {
                throw new ArgumentOutOfRangeException(metadataName);
            }

            using (SqlConnection connection = new SqlConnection(MS_CONNECTION_STRING))
            {
                connection.Open();

                using (SqlCommand command = connection.CreateCommand())
                {
                    command.CommandType = CommandType.Text;

                    string insert = $"INSERT {queue.TableName}";
                    List<string> fields = new List<string>();
                    List<string> values = new List<string>();
                    foreach (MetadataProperty property in queue.Properties)
                    {
                        values.Add($"@{property.Name}");
                        fields.Add(property.Fields[0].Name);
                    }
                    command.CommandText = $"{insert} ({string.Join(',', fields)}) VALUES ({string.Join(',', values)});";

                    command.Parameters.AddWithValue("Идентификатор", Guid.NewGuid().ToByteArray());
                    command.Parameters.AddWithValue("ТелоСообщения", Encoding.UTF8.GetString(message.Span));

                    command.ExecuteNonQuery();
                }
            }
        }
        public static void PgSendMessage(string metadataName, ReadOnlyMemory<byte> message)
        {
            if (!new MetadataService()
                .UseConnectionString(PG_CONNECTION_STRING)
                .UseDatabaseProvider(DatabaseProvider.PostgreSQL)
                .TryOpenInfoBase(out InfoBase infoBase, out string error))
            {
                throw new InvalidOperationException(error);
            }

            ApplicationObject queue = infoBase.GetApplicationObjectByName(metadataName);

            if (queue == null)
            {
                throw new ArgumentOutOfRangeException(metadataName);
            }

            using (NpgsqlConnection connection = new NpgsqlConnection(PG_CONNECTION_STRING))
            {
                connection.Open();

                using (NpgsqlCommand command = connection.CreateCommand())
                {
                    command.CommandType = CommandType.Text;

                    string insert = $"INSERT INTO {queue.TableName}";
                    List<string> fields = new List<string>();
                    List<string> values = new List<string>();
                    foreach (MetadataProperty property in queue.Properties)
                    {
                        DatabaseField field = property.Fields[0];

                        fields.Add(field.Name);

                        if (property.PropertyType.CanBeString)
                        {
                            values.Add($"CAST(@{property.Name} AS mvarchar)");
                        }
                        else
                        {
                            values.Add($"@{property.Name}");
                        }
                    }
                    command.CommandText = $"{insert} ({string.Join(',', fields)}) SELECT {string.Join(',', values)};";

                    command.Parameters.AddWithValue("Идентификатор", Guid.NewGuid().ToByteArray());
                    command.Parameters.AddWithValue("ТелоСообщения", Encoding.UTF8.GetString(message.Span));

                    command.ExecuteNonQuery();
                }
            }
        }
    }
}