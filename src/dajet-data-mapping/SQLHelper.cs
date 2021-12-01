using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Text;

namespace DaJet.Data
{
    public sealed class SqlFieldInfo
    {
        public SqlFieldInfo() { }
        public int ORDINAL_POSITION;
        public string COLUMN_NAME;
        public string DATA_TYPE;
        public int CHARACTER_MAXIMUM_LENGTH;
        public byte NUMERIC_PRECISION;
        public int NUMERIC_SCALE;
        public bool IS_NULLABLE;
        public bool IsFound;
    }
    public sealed class ClusteredIndexInfo
    {
        public ClusteredIndexInfo() { }
        public string NAME;
        public bool IS_UNIQUE;
        public bool IS_PRIMARY_KEY;
        public List<ClusteredIndexColumnInfo> COLUMNS = new List<ClusteredIndexColumnInfo>();
        public bool HasNullableColumns
        {
            get
            {
                bool result = false;
                foreach (ClusteredIndexColumnInfo item in COLUMNS)
                {
                    if (item.IS_NULLABLE)
                    {
                        return true;
                    }
                }
                return result;
            }
        }
        public ClusteredIndexColumnInfo GetColumnByName(string name)
        {
            ClusteredIndexColumnInfo info = null;
            for (int i = 0; i < COLUMNS.Count; i++)
            {
                if (COLUMNS[i].NAME == name) return COLUMNS[i];
            }
            return info;
        }
    }
    public sealed class ClusteredIndexColumnInfo
    {
        public ClusteredIndexColumnInfo() { }
        public byte KEY_ORDINAL;
        public string NAME;
        public bool IS_NULLABLE;
        public bool IS_DESCENDING_KEY; // 0 - ASC, 1 - DESC
    }
    public static class SQLHelper
    {   
        public static List<SqlFieldInfo> GetSqlFields(string connectionString, string tableName)
        {
            StringBuilder sb = new StringBuilder();
            sb.AppendLine(@"SELECT");
            sb.AppendLine(@"    ORDINAL_POSITION, COLUMN_NAME, DATA_TYPE,");
            sb.AppendLine(@"    ISNULL(CHARACTER_MAXIMUM_LENGTH, 0) AS CHARACTER_MAXIMUM_LENGTH,");
            sb.AppendLine(@"    ISNULL(NUMERIC_PRECISION, 0) AS NUMERIC_PRECISION,");
            sb.AppendLine(@"    ISNULL(NUMERIC_SCALE, 0) AS NUMERIC_SCALE,");
            sb.AppendLine(@"    CASE WHEN IS_NULLABLE = 'NO' THEN CAST(0x00 AS bit) ELSE CAST(0x01 AS bit) END AS IS_NULLABLE");
            sb.AppendLine(@"FROM");
            sb.AppendLine(@"    INFORMATION_SCHEMA.COLUMNS");
            sb.AppendLine(@"WHERE");
            sb.AppendLine(@"    TABLE_NAME = N'{0}'");
            sb.AppendLine(@"ORDER BY");
            sb.AppendLine(@"    ORDINAL_POSITION ASC;");

            string sql = string.Format(sb.ToString(), tableName);

            List<SqlFieldInfo> list = new List<SqlFieldInfo>();
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                using (SqlCommand command = new SqlCommand(sql, connection))
                {
                    connection.Open();
                    using (SqlDataReader reader = command.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            SqlFieldInfo item = new SqlFieldInfo()
                            {
                                ORDINAL_POSITION = reader.GetInt32(0),
                                COLUMN_NAME = reader.GetString(1),
                                DATA_TYPE = reader.GetString(2),
                                CHARACTER_MAXIMUM_LENGTH = reader.GetInt32(3),
                                NUMERIC_PRECISION = reader.GetByte(4),
                                NUMERIC_SCALE = reader.GetInt32(5),
                                IS_NULLABLE = reader.GetBoolean(6)
                            };
                            list.Add(item);
                        }
                    }
                }
            }
            return list;
        }
        public static ClusteredIndexInfo GetClusteredIndexInfo(string connectionString, string tableName)
        {
            ClusteredIndexInfo info = null;

            StringBuilder sb = new StringBuilder();
            sb.AppendLine(@"SELECT");
            sb.AppendLine(@"    i.name,");
            sb.AppendLine(@"    i.is_unique,");
            sb.AppendLine(@"    i.is_primary_key,");
            sb.AppendLine(@"    c.key_ordinal,");
            sb.AppendLine(@"    c.is_descending_key,");
            sb.AppendLine(@"    f.name,");
            sb.AppendLine(@"    f.is_nullable");
            sb.AppendLine(@"FROM sys.indexes AS i");
            sb.AppendLine(@"INNER JOIN sys.tables AS t ON t.object_id = i.object_id");
            sb.AppendLine(@"INNER JOIN sys.index_columns AS c ON c.object_id = t.object_id AND c.index_id = i.index_id");
            sb.AppendLine(@"INNER JOIN sys.columns AS f ON f.object_id = t.object_id AND f.column_id = c.column_id");
            sb.AppendLine(@"WHERE");
            sb.AppendLine(@"    t.object_id = OBJECT_ID(@table) AND i.type = 1 -- CLUSTERED");
            sb.AppendLine(@"ORDER BY");
            sb.AppendLine(@"c.key_ordinal ASC;");
            string sql = sb.ToString();

            using (SqlConnection connection = new SqlConnection(connectionString))
            using (SqlCommand command = new SqlCommand(sql, connection))
            {
                connection.Open();

                command.Parameters.AddWithValue("table", tableName);
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        info = new ClusteredIndexInfo()
                        {
                            NAME = reader.GetString(0),
                            IS_UNIQUE = reader.GetBoolean(1),
                            IS_PRIMARY_KEY = reader.GetBoolean(2)
                        };
                        info.COLUMNS.Add(new ClusteredIndexColumnInfo()
                        {
                            KEY_ORDINAL = reader.GetByte(3),
                            IS_DESCENDING_KEY = reader.GetBoolean(4),
                            NAME = reader.GetString(5),
                            IS_NULLABLE = reader.GetBoolean(6)
                        });
                        while (reader.Read())
                        {
                            info.COLUMNS.Add(new ClusteredIndexColumnInfo()
                            {
                                KEY_ORDINAL = reader.GetByte(3),
                                IS_DESCENDING_KEY = reader.GetBoolean(4),
                                NAME = reader.GetString(5),
                                IS_NULLABLE = reader.GetBoolean(6)
                            });
                        }
                    }
                }
            }
            return info;
        }
        public static byte[] Get1CUuid(byte[] uuid_sql)
        {
            // CAST(REVERSE(SUBSTRING(@uuid_sql, 9, 8)) AS binary(8)) + SUBSTRING(@uuid_sql, 1, 8)

            byte[] uuid_1c = new byte[16];

            for (int i = 0; i < 8; i++)
            {
                uuid_1c[i] = uuid_sql[15 - i];
                uuid_1c[8 + i] = uuid_sql[i];
            }

            return uuid_1c;
        }
        public static byte[] GetSqlUuid(byte[] uuid_1c)
        {
            byte[] uuid_sql = new byte[16];

            for (int i = 0; i < 8; i++)
            {
                uuid_sql[i] = uuid_1c[8 + i];
                uuid_sql[8 + i] = uuid_1c[7 - i];
            }

            return uuid_sql;
        }
        public static byte[] GetSqlUuid(Guid guid_1c)
        {
            return GetSqlUuid(guid_1c.ToByteArray());
        }
    }
}