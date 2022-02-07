using DaJet.Data.Mapping;
using DaJet.Metadata.Model;
using Microsoft.IO;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Unicode;

namespace DaJet.Json
{
    public sealed class EntityJsonSerializer : IDaJetJsonSerializer
    {
        private const string CONST_REF = "Ref";
        private const string CONST_TYPE = "#type";
        private const string CONST_VALUE = "#value";
        private const string CONST_TYPE_STRING = "jxs:string";
        private const string CONST_TYPE_DECIMAL = "jxs:decimal";
        private const string CONST_TYPE_BOOLEAN = "jxs:boolean";
        private const string CONST_TYPE_DATETIME = "jxs:dateTime";
        private const string CONST_TYPE_CATALOG_REF = "jcfg:CatalogRef";
        private const string CONST_TYPE_CATALOG_OBJ = "jcfg:CatalogObject";
        private const string CONST_TYPE_DOCUMENT_REF = "jcfg:DocumentRef";
        private const string CONST_TYPE_DOCUMENT_OBJ = "jcfg:DocumentObject";
        private const string CONST_TYPE_OBJECT_DELETION = "jent:ObjectDeletion";

        private readonly RecyclableMemoryStreamManager StreamManager = new RecyclableMemoryStreamManager();

        private EntityDataMapper DataMapper { get; set; }
        private EntityDataMapperProvider DataMapperProvider { get; set; }
        private Dictionary<string, string> PropertyAliases { get; } = new Dictionary<string, string>()
        {
            { "Ссылка",           "Ref" },                // Catalog & Document
            { "ПометкаУдаления",  "DeletionMark" },       // Catalog & Document
            { "Владелец",         "Owner" },              // Catalog
            { "Код",              "Code" },               // Catalog
            { "Наименование",     "Description" },        // Catalog
            { "Родитель",         "Parent" },             // Catalog
            { "ЭтоГруппа",        "IsFolder" },           // Catalog
            { "Предопределённый", "PredefinedDataName" }, // Catalog
            { "Дата",             "Date" },               // Document
            { "Номер",            "Number" },             // Document
            { "Проведён",         "Posted" }              // Document
        };

        public EntityJsonSerializer(EntityDataMapper mapper)
        {
            DataMapper = mapper;
        }
        public EntityJsonSerializer(EntityDataMapperProvider provider)
        {
            DataMapperProvider = provider;
        }

        public IEnumerable<ReadOnlyMemory<byte>> Serialize(int pageSize, int pageNumber)
        {
            foreach (IDataReader reader in DataMapper.GetPageDataRows(pageSize, pageNumber))
            {
                yield return Serialize(reader);
            }
        }
        public ReadOnlyMemory<byte> Serialize(Guid entity)
        {
            ReadOnlyMemory<byte> jdto = null;

            foreach (IDataReader reader in DataMapper.GetEntityByUuid(entity))
            {
                jdto = Serialize(reader);
            }
            
            return jdto;
        }
        public ReadOnlyMemory<byte> Serialize(string metadataName, Guid entity)
        {
            if (!DataMapperProvider.TryGetDataMapper(metadataName, out EntityDataMapper mapper))
            {
                throw new ArgumentOutOfRangeException(nameof(metadataName));
            }

            DataMapper = mapper;

            ReadOnlyMemory<byte> jdto = null;

            foreach (IDataReader reader in DataMapper.GetEntityByUuid(entity))
            {
                jdto = Serialize(reader);
            }

            return jdto;
        }
        public ReadOnlyMemory<byte> SerializeAsObjectDeletion(string metadataName, Guid entity)
        {
            if (!DataMapperProvider.TryGetDataMapper(metadataName, out EntityDataMapper mapper))
            {
                throw new ArgumentOutOfRangeException(nameof(metadataName));
            }

            DataMapper = mapper;

            ReadOnlyMemory<byte> jdto;

            JsonWriterOptions options = new JsonWriterOptions
            {
                Indented = false,
                Encoder = JavaScriptEncoder.Create(UnicodeRanges.All)
            };

            using (MemoryStream stream = StreamManager.GetStream())
            using (Utf8JsonWriter writer = new Utf8JsonWriter(stream, options))
            {
                writer.Reset();
                stream.Position = 0;

                writer.WriteStartObject(); // start of data transfer object

                writer.WriteString(CONST_TYPE, CONST_TYPE_OBJECT_DELETION);

                writer.WritePropertyName(CONST_VALUE);
                writer.WriteStartObject(); // start of entity value

                writer.WritePropertyName(CONST_REF);
                writer.WriteStartObject(); // start of entity reference

                if (DataMapper.Options.MetaObject is Catalog)
                {
                    writer.WriteString(CONST_TYPE, CONST_TYPE_CATALOG_REF + "." + DataMapper.Options.MetaObject.Name);
                }
                else if (DataMapper.Options.MetaObject is Document)
                {
                    writer.WriteString(CONST_TYPE, CONST_TYPE_DOCUMENT_REF + "." + DataMapper.Options.MetaObject.Name);
                }
                else
                {
                    throw new ArgumentOutOfRangeException(nameof(metadataName));
                }

                writer.WriteString(CONST_VALUE, entity.ToString());

                writer.WriteEndObject(); // end of entity reference
                writer.WriteEndObject(); // end of entity value
                writer.WriteEndObject(); // end of data transfer object

                writer.Flush();

                jdto = new ReadOnlyMemory<byte>(stream.GetBuffer(), 0, (int)writer.BytesCommitted);
            }

            return jdto;
        }
        private ReadOnlyMemory<byte> Serialize(IDataReader reader)
        {
            ReadOnlyMemory<byte> bytes;

            JsonWriterOptions options = new JsonWriterOptions
            {
                Indented = false,
                Encoder = JavaScriptEncoder.Create(UnicodeRanges.All)
            };

            using (MemoryStream stream = StreamManager.GetStream())
            using (Utf8JsonWriter writer = new Utf8JsonWriter(stream, options))
            {
                writer.Reset();
                stream.Position = 0;

                SerializeDataToJson(reader, writer);

                writer.Flush();

                bytes = new ReadOnlyMemory<byte>(stream.GetBuffer(), 0, (int)writer.BytesCommitted);
            }

            return bytes;
        }
        private void SerializeDataToJson(IDataReader reader, Utf8JsonWriter writer)
        {
            EntityRef entity = DataMapper.GetEntityRef(reader);
            bool isFolder = DataMapper.GetIsFolder(reader);

            writer.WriteStartObject(); // start of data transfer object
            if (DataMapper.Options.MetaObject is Catalog)
            {
                writer.WriteString(CONST_TYPE, CONST_TYPE_CATALOG_OBJ + "." + DataMapper.Options.MetaObject.Name);
            }
            else
            {
                writer.WriteString(CONST_TYPE, CONST_TYPE_DOCUMENT_OBJ + "." + DataMapper.Options.MetaObject.Name);
            }
            
            writer.WritePropertyName(CONST_VALUE);
            writer.WriteStartObject(); // start of entity

            for (int i = 0; i < DataMapper.PropertyMappers.Count; i++) // entity properties
            {
                object value = DataMapper.PropertyMappers[i].GetValue(reader);

                if (!IsPropertySerializable(DataMapper.PropertyMappers[i], isFolder, value))
                {
                    continue;
                }

                if (DataMapper.Options.MetaObject is Catalog || DataMapper.Options.MetaObject is Characteristic)
                {
                    if (DataMapper.PropertyMappers[i].Property.Name == "Предопределённый")
                    {
                        value = DataMapper.GetPredefinedDataName(reader, (Guid)value);
                    }
                    else if (DataMapper.PropertyMappers[i].Property.Name == "Владелец")
                    {
                        if (!PropertyAliases.TryGetValue(DataMapper.PropertyMappers[i].Property.Name, out string propertyName))
                        {
                            propertyName = DataMapper.PropertyMappers[i].Property.Name;
                        }
                        WriteObjectValueToJson(writer, propertyName, value);
                        continue;
                    }
                }

                WriteValueToJson(writer, DataMapper.PropertyMappers[i], value);
            }

            foreach (EntityDataMapper table in DataMapper.Options.TablePartMappers) // table parts
            {
                int rowCount = 0; // Empty table part is not serialized !

                foreach (IDataReader record in table.GetTablePartDataRows(entity))
                {
                    if (rowCount == 0)
                    {
                        writer.WritePropertyName(table.Options.MetaObject.Name);
                        writer.WriteStartArray(); // start of table part
                    }

                    writer.WriteStartObject(); // start of record
                    for (int i = 0; i < table.PropertyMappers.Count; i++)
                    {
                        object value = table.PropertyMappers[i].GetValue(record);

                        WriteValueToJson(writer, table.PropertyMappers[i], value);
                    }
                    writer.WriteEndObject(); // end of record

                    rowCount++;
                }

                if (rowCount > 0)
                {
                    writer.WriteEndArray(); // end of table part
                }
            }

            writer.WriteEndObject(); // end of entity

            writer.WriteEndObject(); // end of data transfer object
        }
        private bool IsPropertySerializable(PropertyMapper mapper, bool isFolder, object value)
        {
            if (mapper.Property is SharedProperty)
            {
                return true;
            }

            if (mapper.Property.Name == "Предопределённый" && (Guid)value == Guid.Empty)
            {
                return false;
            }

            if (PropertyAliases.TryGetValue(mapper.Property.Name, out _))
            {
                return true;
            }

            if (isFolder)
            {
                if (mapper.Property.PropertyUsage == PropertyUsage.Item)
                {
                    return false; // do not serialize
                }
            }
            else
            {
                if (mapper.Property.PropertyUsage == PropertyUsage.Folder)
                {
                    return false; // do not serialize
                }
            }
            return true;
        }
        private void WriteValueToJson(Utf8JsonWriter writer, PropertyMapper mapper, object value)
        {
            if (!PropertyAliases.TryGetValue(mapper.Property.Name, out string propertyName))
            {
                propertyName = mapper.Property.Name;
            }

            if (mapper.DiscriminatorOrdinal > -1)
            {
                WriteMultipleValueToJson(writer, propertyName, value);
            }
            else if (mapper.TypeCodeOrdinal > -1)
            {
                WriteObjectValueToJson(writer, propertyName, value);
            }
            else
            {
                WriteSingleValueToJson(writer, propertyName, value);
            }
        }
        private void WriteSingleValueToJson(Utf8JsonWriter writer, string propertyName, object value)
        {
            if (value == null)
            {
                writer.WriteNull(propertyName);
            }
            else if (value is Guid)
            {
                writer.WriteString(propertyName, value.ToString());
            }
            else if (value is byte[] array)
            {
                writer.WriteString(propertyName, Convert.ToBase64String(array));
            }
            else if (value is string text)
            {
                writer.WriteString(propertyName, text);
            }
            else if (value is bool boolean)
            {
                writer.WriteBoolean(propertyName, boolean);
            }
            else if (value is decimal numeric)
            {
                writer.WriteNumber(propertyName, numeric);
            }
            else if (value is DateTime dateTime)
            {
                writer.WriteString(propertyName, dateTime.ToString("yyyy-MM-ddTHH:mm:ss"));
            }
            else if (value is EntityRef entity)
            {
                if (entity.IsEnum)
                {
                    writer.WriteString(propertyName, entity.EnumValue);
                }
                else
                {
                    writer.WriteString(propertyName, entity.Identity.ToString());
                }
            }
            // TODO: unsupported value type
        }
        private void WriteMultipleValueToJson(Utf8JsonWriter writer, string propertyName, object value)
        {
            if (value == null)
            {
                writer.WriteNull(propertyName);
            }
            else if (value is EntityRef entity)
            {
                WriteObjectValueToJson(writer, propertyName, entity);
            }
            else if (value is string text)
            {
                writer.WritePropertyName(propertyName);
                writer.WriteStartObject();
                writer.WriteString(CONST_TYPE, CONST_TYPE_STRING);
                writer.WriteString(CONST_VALUE, text);
                writer.WriteEndObject();
            }
            else if (value is bool boolean)
            {
                writer.WritePropertyName(propertyName);
                writer.WriteStartObject();
                writer.WriteString(CONST_TYPE, CONST_TYPE_BOOLEAN);
                writer.WriteBoolean(CONST_VALUE, boolean);
                writer.WriteEndObject();
            }
            else if (value is decimal numeric)
            {
                writer.WritePropertyName(propertyName);
                writer.WriteStartObject();
                writer.WriteString(CONST_TYPE, CONST_TYPE_DECIMAL);
                writer.WriteNumber(CONST_VALUE, numeric);
                writer.WriteEndObject();
            }
            else if (value is DateTime dateTime)
            {
                writer.WritePropertyName(propertyName);
                writer.WriteStartObject();
                writer.WriteString(CONST_TYPE, CONST_TYPE_DATETIME);
                writer.WriteString(CONST_VALUE, dateTime.ToString("yyyy-MM-ddTHH:mm:ss"));
                writer.WriteEndObject();
            }
        }
        private void WriteObjectValueToJson(Utf8JsonWriter writer, string propertyName, object value)
        {
            EntityRef entity = value as EntityRef;

            writer.WritePropertyName(propertyName);
            writer.WriteStartObject();
            writer.WriteString(CONST_TYPE, entity.TypeName);
            if (entity.IsEnum)
            {
                writer.WriteString(CONST_VALUE, entity.EnumValue);
            }
            else
            {
                writer.WriteString(CONST_VALUE, entity.Identity.ToString());
            }
            writer.WriteEndObject();
        }
    }
}