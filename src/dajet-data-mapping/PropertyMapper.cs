using DaJet.Metadata.Model;
using System;
using System.Data;
using System.Text;

namespace DaJet.Data.Mapping
{
    public sealed class PropertyMapper
    {
        private const string CONST_TYPE_ENUM = "jcfg:EnumRef";
        private const string CONST_TYPE_CATALOG = "jcfg:CatalogRef";
        private const string CONST_TYPE_DOCUMENT = "jcfg:DocumentRef";

        private int ValueOrdinal = -1;
        private int NumberOrdinal = -1;
        private int StringOrdinal = -1;
        private int ObjectOrdinal = -1;
        private int BooleanOrdinal = -1;
        private int DateTimeOrdinal = -1;
        public int TypeCodeOrdinal { get; private set; } = -1;
        public int DiscriminatorOrdinal { get; private set; } = -1;

        public InfoBase InfoBase { get; }
        public MetadataProperty Property { get; }
        public Enumeration Enumeration { get; private set; }

        internal PropertyMapper(InfoBase infoBase, MetadataProperty property)
        {
            InfoBase = infoBase;
            Property = property;
        }
        internal void Initialize(ref int ordinal)
        {
            if (InfoBase.ReferenceTypeUuids.TryGetValue(Property.PropertyType.ReferenceTypeUuid, out ApplicationObject metaObject))
            {
                Enumeration = metaObject as Enumeration;
            }

            for (int i = 0; i < Property.Fields.Count; i++)
            {
                ordinal++;

                FieldPurpose purpose = Property.Fields[i].Purpose;

                if (purpose == FieldPurpose.Value)
                {
                    ValueOrdinal = ordinal; // 
                }
                else if (purpose == FieldPurpose.Discriminator)
                {
                    DiscriminatorOrdinal = ordinal; // binary(1) -> byte
                    // 0x01 - Неопределено -> null     -> null
                    // 0x02 - Булево       -> bool     -> jxs:boolean  + true | false
                    // 0x03 - Число        -> decimal  -> jxs:decimal  + numeric
                    // 0x04 - Дата         -> DateTime -> jxs:dateTime + string (ISO 8601)
                    // 0x05 - Строка       -> string   -> jxs:string   + string
                    // 0x08 - Ссылка       -> Guid     -> jcfg:EnumRef     + Name
                    //                                  | jcfg:CatalogRef  + UUID
                    // EntityRef { TypeCode, Identity } | jcfg:DocumentRef + UUID
                }
                else if (purpose == FieldPurpose.TypeCode)
                {
                    TypeCodeOrdinal = ordinal; // binary(4) -> int
                }
                else if (purpose == FieldPurpose.String)
                {
                    StringOrdinal = ordinal; // nvarchar | nchar -> string
                }
                else if (purpose == FieldPurpose.Boolean)
                {
                    BooleanOrdinal = ordinal; // binary(1) -> 0x00 | 0x01 -> bool
                }
                else if (purpose == FieldPurpose.Object)
                {
                    ObjectOrdinal = ordinal; // binary(16) -> Guid
                }
                else if (purpose == FieldPurpose.Numeric)
                {
                    NumberOrdinal = ordinal; // numeric -> decimal | int | long
                }
                else if (purpose == FieldPurpose.DateTime)
                {
                    DateTimeOrdinal = ordinal; // datetime2 -> DateTime
                }
                else
                {
                    // TODO: ingnore this kind of the field purpose !?
                }
            }
        }
        
        internal void BuildSelectCommand(StringBuilder script, string tableAlias)
        {
            for (int i = 0; i < Property.Fields.Count; i++)
            {
                if (Property.Fields[i].Purpose == FieldPurpose.TypeCode ||
                    Property.Fields[i].Purpose == FieldPurpose.Discriminator)
                {
                    script.Append("CAST(");
                }

                if (string.IsNullOrEmpty(tableAlias))
                {
                    script.Append(Property.Fields[i].Name);
                }
                else
                {
                    script.Append($"{tableAlias}.{Property.Fields[i].Name}");
                }

                if (Property.Fields[i].Purpose == FieldPurpose.TypeCode ||
                    Property.Fields[i].Purpose == FieldPurpose.Discriminator)
                {
                    script.Append(" AS int)");
                }

                script.Append($" AS [{Property.Name}], ");
            }
        }

        public object GetValue(IDataReader reader)
        {
            if (DiscriminatorOrdinal > -1)
            {
                return GetMultipleValue(reader);
            }
            else if (TypeCodeOrdinal > -1)
            {
                return GetObjectValue(reader);
            }
            return GetSingleValue(reader);
        }
        private string GetEnumValue(Guid uuid)
        {
            if (Enumeration == null) return string.Empty;

            for (int i = 0; i < Enumeration.Values.Count; i++)
            {
                if (Enumeration.Values[i].Uuid == uuid)
                {
                    return Enumeration.Values[i].Name;
                }
            }

            return string.Empty;
        }
        private object GetSingleValue(IDataReader reader)
        {
            if (reader.IsDBNull(ValueOrdinal))
            {
                return null;
            }

            if (Property.PropertyType.IsUuid) // УникальныйИдентификатор
            {
                return new Guid(SQLHelper.Get1CUuid((byte[])reader.GetValue(ValueOrdinal)));
            }
            else if (Property.PropertyType.IsValueStorage) // ХранилищеЗначения
            {
                return ((byte[])reader.GetValue(ValueOrdinal));
            }
            else if (Property.PropertyType.CanBeString)
            {
                return reader.GetString(ValueOrdinal);
            }
            else if (Property.PropertyType.CanBeBoolean)
            {
                if (Property.Purpose == PropertyPurpose.System && Property.Name == "ЭтоГруппа")
                {
                    return (((byte[])reader.GetValue(ValueOrdinal))[0] == 0); // Unique 1C case =)
                }
                return (((byte[])reader.GetValue(ValueOrdinal))[0] != 0); // All other cases
            }
            else if (Property.PropertyType.CanBeNumeric)
            {
                return reader.GetDecimal(ValueOrdinal);
            }
            else if (Property.PropertyType.CanBeDateTime)
            {
                DateTime dateTime = reader.GetDateTime(ValueOrdinal);
                if (dateTime.Year > 4000)
                {
                    dateTime = dateTime.AddYears(-2000);
                }
                return dateTime;
            }
            else if (Property.PropertyType.CanBeReference)
            {
                Guid uuid = new Guid(SQLHelper.Get1CUuid((byte[])reader.GetValue(ValueOrdinal)));

                if (Property.PropertyType.ReferenceTypeCode == 0) // TODO: should be fixed in DaJet.Metadata library
                {
                    // NOTE: Property.PropertyType.ReferenceTypeUuid for Owner property is a FileName, not metdata object Uuid !!!
                    if (InfoBase.Catalogs.TryGetValue(Property.PropertyType.ReferenceTypeUuid, out ApplicationObject owner1))
                    {
                        Property.PropertyType.ReferenceTypeCode = owner1.TypeCode;
                    }
                    else if (InfoBase.Characteristics.TryGetValue(Property.PropertyType.ReferenceTypeUuid, out ApplicationObject owner2))
                    {
                        Property.PropertyType.ReferenceTypeCode = owner2.TypeCode;
                    }
                }

                if (Enumeration != null)
                {
                    return new EntityRef(Property.PropertyType.ReferenceTypeCode, uuid, Enumeration.Name, GetEnumValue(uuid));
                }
                else
                {
                    return new EntityRef(Property.PropertyType.ReferenceTypeCode, uuid);
                }
            }
            
            // TODO: undefined property type ?
            return null;
        }
        private object GetMultipleValue(IDataReader reader)
        {
            int discriminator = reader.GetInt32(DiscriminatorOrdinal);

            if (discriminator == 1) // Неопределено
            {
                return null;
            }
            else if (discriminator == 2) // Булево
            {
                return (((byte[])reader.GetValue(BooleanOrdinal))[0] != 0);
            }
            else if (discriminator == 3) // Число
            {
                return reader.GetDecimal(NumberOrdinal);
            }
            else if (discriminator == 4) // Дата
            {
                DateTime dateTime = reader.GetDateTime(DateTimeOrdinal);
                if (dateTime.Year > 4000)
                {
                    dateTime = dateTime.AddYears(-2000);
                }
                return dateTime;
            }
            else if (discriminator == 5) // Строка
            {
                return reader.GetString(StringOrdinal);
            }
            else if (discriminator == 8) // Ссылка
            {
                return GetObjectValue(reader);
            }
            
            // TODO: unknown discriminator ?
            return null;
        }
        private object GetObjectValue(IDataReader reader)
        {
            int typeCode = reader.GetInt32(TypeCodeOrdinal);
            
            Guid uuid = new Guid(SQLHelper.Get1CUuid((byte[])reader.GetValue(ObjectOrdinal)));

            return GetEntityRef(typeCode, uuid);
        }
        public EntityRef GetEntityRef(int typeCode, Guid uuid)
        {
            if (InfoBase.ReferenceTypeCodes.TryGetValue(typeCode, out ApplicationObject metaObject))
            {
                if (metaObject is Enumeration enumeration)
                {
                    return new EntityRef(typeCode, uuid, CONST_TYPE_ENUM + "." + enumeration.Name, GetEnumValue(uuid));
                }
                else if (metaObject is Catalog catalog)
                {
                    return new EntityRef(typeCode, uuid, CONST_TYPE_CATALOG + "." + catalog.Name);
                }
                else if (metaObject is Document document)
                {
                    return new EntityRef(typeCode, uuid, CONST_TYPE_DOCUMENT + "." + document.Name);
                }
            }

            // TODO: unsupported reference type ?
            return null;
        }
    }
}