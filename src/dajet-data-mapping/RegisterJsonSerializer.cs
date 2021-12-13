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
    public sealed class RegisterJsonSerializer : IDaJetJsonSerializer
    {
        private const string CONST_TYPE = "#type";
        private const string CONST_VALUE = "#value";
        private const string CONST_TYPE_STRING = "jxs:string";
        private const string CONST_TYPE_DECIMAL = "jxs:decimal";
        private const string CONST_TYPE_BOOLEAN = "jxs:boolean";
        private const string CONST_TYPE_DATETIME = "jxs:dateTime";
        private const string CONST_TYPE_INFO_REGISTER_SET = "jcfg:InformationRegisterRecordSet";
        private const string CONST_TYPE_ACCUM_REGISTER_SET = "jcfg:AccumulationRegisterRecordSet";

        private readonly RecyclableMemoryStreamManager StreamManager = new RecyclableMemoryStreamManager();

        public RegisterDataMapper DataMapper { get; private set; }
        private List<MetadataProperty> Selection { get; set; } // Отбор набора записей
        private Dictionary<string, string> PropertyAliases { get; set; }

        public RegisterJsonSerializer(RegisterDataMapper mapper)
        {
            DataMapper = mapper;

            Selection = DataMapper.GetSelectionProperties();

            if (DataMapper.Options.MetaObject is InformationRegister ||
                DataMapper.Options.MetaObject is AccumulationRegister)
            {
                PropertyAliases = new Dictionary<string, string>()
                {
                    { "Регистратор", "Recorder" },
                    { "Период",      "Period" },
                    { "ВидДвижения", "RecordType" },
                    { "Активность",  "Active" }
                };
            }
        }

        public IEnumerable<ReadOnlyMemory<byte>> Serialize(int pageSize, int pageNumber)
        {
            foreach (IDataReader reader in DataMapper.GetPageDataRows(pageSize, pageNumber))
            {
                yield return Serialize(reader);
            }
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
            writer.WriteStartObject(); // start of data transfer object
            if (DataMapper.Options.MetaObject is InformationRegister)
            {
                writer.WriteString(CONST_TYPE, CONST_TYPE_INFO_REGISTER_SET + "." + DataMapper.Options.MetaObject.Name);
            }
            else
            {
                writer.WriteString(CONST_TYPE, CONST_TYPE_ACCUM_REGISTER_SET + "." + DataMapper.Options.MetaObject.Name);
            }
            
            writer.WritePropertyName(CONST_VALUE);
            writer.WriteStartObject(); // start of record set

            writer.WritePropertyName("Filter");
            writer.WriteStartArray(); // start of filter
            WriteSelectionToJson(reader, writer);
            writer.WriteEndArray(); // end of filter

            writer.WritePropertyName("Record");
            writer.WriteStartArray(); // start of records
            WriteRecordToJson(reader, writer);
            writer.WriteEndArray(); // end of records

            writer.WriteEndObject(); // end of record set

            writer.WriteEndObject(); // end of data transfer object
        }

        private PropertyMapper FindMapperByProperty(MetadataProperty property)
        {
            for (int i = 0; i < DataMapper.PropertyMappers.Count; i++)
            {
                if (DataMapper.PropertyMappers[i].Property == property)
                {
                    return DataMapper.PropertyMappers[i];
                }
            }
            return null;
        }
        private void WriteSelectionToJson(IDataReader reader, Utf8JsonWriter writer)
        {
            foreach (MetadataProperty property in Selection)
            {
                PropertyMapper mapper = FindMapperByProperty(property);

                if (mapper != null)
                {
                    WriteSelectionParameterToJson(reader, writer, mapper);
                }
            }
        }
        private void WriteSelectionParameterToJson(IDataReader reader, Utf8JsonWriter writer, PropertyMapper mapper)
        {
            if (!PropertyAliases.TryGetValue(mapper.Property.Name, out string propertyName))
            {
                propertyName = mapper.Property.Name;
            }

            object value = mapper.GetValue(reader);

            writer.WriteStartObject(); // start of filter parameter

            writer.WritePropertyName("Name");
            writer.WriteStartObject();
            writer.WriteString(CONST_TYPE, CONST_TYPE_STRING);
            writer.WriteString(CONST_VALUE, propertyName);
            writer.WriteEndObject();

            WriteMultipleValueToJson(writer, "Value", value);

            writer.WriteEndObject(); // end of filter item parameter
        }
        private void WriteRecordToJson(IDataReader reader, Utf8JsonWriter writer)
        {
            writer.WriteStartObject(); // start of record

            for (int i = 0; i < DataMapper.PropertyMappers.Count; i++) // record properties
            {
                object value = DataMapper.PropertyMappers[i].GetValue(reader);

                if (DataMapper.Options.MetaObject is AccumulationRegister register)
                {
                    if (register.RegisterKind == RegisterKind.Balance && DataMapper.PropertyMappers[i].Property.Name == "ВидДвижения")
                    {
                        value = (decimal)value == 0 ? "Receipt" : "Expense";
                    }
                }

                WriteValueToJson(writer, DataMapper.PropertyMappers[i], value);
            }

            writer.WriteEndObject(); // end of record
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
            // unsupported value type - this should not happen
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