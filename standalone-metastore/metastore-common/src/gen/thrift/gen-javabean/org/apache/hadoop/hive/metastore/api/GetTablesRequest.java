/**
 * Autogenerated by Thrift Compiler (0.13.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.hadoop.hive.metastore.api;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.13.0)")
@org.apache.hadoop.classification.InterfaceAudience.Public @org.apache.hadoop.classification.InterfaceStability.Stable public class GetTablesRequest implements org.apache.thrift.TBase<GetTablesRequest, GetTablesRequest._Fields>, java.io.Serializable, Cloneable, Comparable<GetTablesRequest> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("GetTablesRequest");

  private static final org.apache.thrift.protocol.TField DB_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("dbName", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField TBL_NAMES_FIELD_DESC = new org.apache.thrift.protocol.TField("tblNames", org.apache.thrift.protocol.TType.LIST, (short)2);
  private static final org.apache.thrift.protocol.TField CAPABILITIES_FIELD_DESC = new org.apache.thrift.protocol.TField("capabilities", org.apache.thrift.protocol.TType.STRUCT, (short)3);
  private static final org.apache.thrift.protocol.TField CAT_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("catName", org.apache.thrift.protocol.TType.STRING, (short)4);
  private static final org.apache.thrift.protocol.TField PROCESSOR_CAPABILITIES_FIELD_DESC = new org.apache.thrift.protocol.TField("processorCapabilities", org.apache.thrift.protocol.TType.LIST, (short)5);
  private static final org.apache.thrift.protocol.TField PROCESSOR_IDENTIFIER_FIELD_DESC = new org.apache.thrift.protocol.TField("processorIdentifier", org.apache.thrift.protocol.TType.STRING, (short)6);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new GetTablesRequestStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new GetTablesRequestTupleSchemeFactory();

  private @org.apache.thrift.annotation.Nullable java.lang.String dbName; // required
  private @org.apache.thrift.annotation.Nullable java.util.List<java.lang.String> tblNames; // optional
  private @org.apache.thrift.annotation.Nullable ClientCapabilities capabilities; // optional
  private @org.apache.thrift.annotation.Nullable java.lang.String catName; // optional
  private @org.apache.thrift.annotation.Nullable java.util.List<java.lang.String> processorCapabilities; // optional
  private @org.apache.thrift.annotation.Nullable java.lang.String processorIdentifier; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    DB_NAME((short)1, "dbName"),
    TBL_NAMES((short)2, "tblNames"),
    CAPABILITIES((short)3, "capabilities"),
    CAT_NAME((short)4, "catName"),
    PROCESSOR_CAPABILITIES((short)5, "processorCapabilities"),
    PROCESSOR_IDENTIFIER((short)6, "processorIdentifier");

    private static final java.util.Map<java.lang.String, _Fields> byName = new java.util.HashMap<java.lang.String, _Fields>();

    static {
      for (_Fields field : java.util.EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // DB_NAME
          return DB_NAME;
        case 2: // TBL_NAMES
          return TBL_NAMES;
        case 3: // CAPABILITIES
          return CAPABILITIES;
        case 4: // CAT_NAME
          return CAT_NAME;
        case 5: // PROCESSOR_CAPABILITIES
          return PROCESSOR_CAPABILITIES;
        case 6: // PROCESSOR_IDENTIFIER
          return PROCESSOR_IDENTIFIER;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new java.lang.IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    @org.apache.thrift.annotation.Nullable
    public static _Fields findByName(java.lang.String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final java.lang.String _fieldName;

    _Fields(short thriftId, java.lang.String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public java.lang.String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private static final _Fields optionals[] = {_Fields.TBL_NAMES,_Fields.CAPABILITIES,_Fields.CAT_NAME,_Fields.PROCESSOR_CAPABILITIES,_Fields.PROCESSOR_IDENTIFIER};
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.DB_NAME, new org.apache.thrift.meta_data.FieldMetaData("dbName", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.TBL_NAMES, new org.apache.thrift.meta_data.FieldMetaData("tblNames", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    tmpMap.put(_Fields.CAPABILITIES, new org.apache.thrift.meta_data.FieldMetaData("capabilities", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, ClientCapabilities.class)));
    tmpMap.put(_Fields.CAT_NAME, new org.apache.thrift.meta_data.FieldMetaData("catName", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.PROCESSOR_CAPABILITIES, new org.apache.thrift.meta_data.FieldMetaData("processorCapabilities", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING))));
    tmpMap.put(_Fields.PROCESSOR_IDENTIFIER, new org.apache.thrift.meta_data.FieldMetaData("processorIdentifier", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(GetTablesRequest.class, metaDataMap);
  }

  public GetTablesRequest() {
  }

  public GetTablesRequest(
    java.lang.String dbName)
  {
    this();
    this.dbName = dbName;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public GetTablesRequest(GetTablesRequest other) {
    if (other.isSetDbName()) {
      this.dbName = other.dbName;
    }
    if (other.isSetTblNames()) {
      java.util.List<java.lang.String> __this__tblNames = new java.util.ArrayList<java.lang.String>(other.tblNames);
      this.tblNames = __this__tblNames;
    }
    if (other.isSetCapabilities()) {
      this.capabilities = new ClientCapabilities(other.capabilities);
    }
    if (other.isSetCatName()) {
      this.catName = other.catName;
    }
    if (other.isSetProcessorCapabilities()) {
      java.util.List<java.lang.String> __this__processorCapabilities = new java.util.ArrayList<java.lang.String>(other.processorCapabilities);
      this.processorCapabilities = __this__processorCapabilities;
    }
    if (other.isSetProcessorIdentifier()) {
      this.processorIdentifier = other.processorIdentifier;
    }
  }

  public GetTablesRequest deepCopy() {
    return new GetTablesRequest(this);
  }

  @Override
  public void clear() {
    this.dbName = null;
    this.tblNames = null;
    this.capabilities = null;
    this.catName = null;
    this.processorCapabilities = null;
    this.processorIdentifier = null;
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getDbName() {
    return this.dbName;
  }

  public void setDbName(@org.apache.thrift.annotation.Nullable java.lang.String dbName) {
    this.dbName = dbName;
  }

  public void unsetDbName() {
    this.dbName = null;
  }

  /** Returns true if field dbName is set (has been assigned a value) and false otherwise */
  public boolean isSetDbName() {
    return this.dbName != null;
  }

  public void setDbNameIsSet(boolean value) {
    if (!value) {
      this.dbName = null;
    }
  }

  public int getTblNamesSize() {
    return (this.tblNames == null) ? 0 : this.tblNames.size();
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Iterator<java.lang.String> getTblNamesIterator() {
    return (this.tblNames == null) ? null : this.tblNames.iterator();
  }

  public void addToTblNames(java.lang.String elem) {
    if (this.tblNames == null) {
      this.tblNames = new java.util.ArrayList<java.lang.String>();
    }
    this.tblNames.add(elem);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.List<java.lang.String> getTblNames() {
    return this.tblNames;
  }

  public void setTblNames(@org.apache.thrift.annotation.Nullable java.util.List<java.lang.String> tblNames) {
    this.tblNames = tblNames;
  }

  public void unsetTblNames() {
    this.tblNames = null;
  }

  /** Returns true if field tblNames is set (has been assigned a value) and false otherwise */
  public boolean isSetTblNames() {
    return this.tblNames != null;
  }

  public void setTblNamesIsSet(boolean value) {
    if (!value) {
      this.tblNames = null;
    }
  }

  @org.apache.thrift.annotation.Nullable
  public ClientCapabilities getCapabilities() {
    return this.capabilities;
  }

  public void setCapabilities(@org.apache.thrift.annotation.Nullable ClientCapabilities capabilities) {
    this.capabilities = capabilities;
  }

  public void unsetCapabilities() {
    this.capabilities = null;
  }

  /** Returns true if field capabilities is set (has been assigned a value) and false otherwise */
  public boolean isSetCapabilities() {
    return this.capabilities != null;
  }

  public void setCapabilitiesIsSet(boolean value) {
    if (!value) {
      this.capabilities = null;
    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getCatName() {
    return this.catName;
  }

  public void setCatName(@org.apache.thrift.annotation.Nullable java.lang.String catName) {
    this.catName = catName;
  }

  public void unsetCatName() {
    this.catName = null;
  }

  /** Returns true if field catName is set (has been assigned a value) and false otherwise */
  public boolean isSetCatName() {
    return this.catName != null;
  }

  public void setCatNameIsSet(boolean value) {
    if (!value) {
      this.catName = null;
    }
  }

  public int getProcessorCapabilitiesSize() {
    return (this.processorCapabilities == null) ? 0 : this.processorCapabilities.size();
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Iterator<java.lang.String> getProcessorCapabilitiesIterator() {
    return (this.processorCapabilities == null) ? null : this.processorCapabilities.iterator();
  }

  public void addToProcessorCapabilities(java.lang.String elem) {
    if (this.processorCapabilities == null) {
      this.processorCapabilities = new java.util.ArrayList<java.lang.String>();
    }
    this.processorCapabilities.add(elem);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.List<java.lang.String> getProcessorCapabilities() {
    return this.processorCapabilities;
  }

  public void setProcessorCapabilities(@org.apache.thrift.annotation.Nullable java.util.List<java.lang.String> processorCapabilities) {
    this.processorCapabilities = processorCapabilities;
  }

  public void unsetProcessorCapabilities() {
    this.processorCapabilities = null;
  }

  /** Returns true if field processorCapabilities is set (has been assigned a value) and false otherwise */
  public boolean isSetProcessorCapabilities() {
    return this.processorCapabilities != null;
  }

  public void setProcessorCapabilitiesIsSet(boolean value) {
    if (!value) {
      this.processorCapabilities = null;
    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getProcessorIdentifier() {
    return this.processorIdentifier;
  }

  public void setProcessorIdentifier(@org.apache.thrift.annotation.Nullable java.lang.String processorIdentifier) {
    this.processorIdentifier = processorIdentifier;
  }

  public void unsetProcessorIdentifier() {
    this.processorIdentifier = null;
  }

  /** Returns true if field processorIdentifier is set (has been assigned a value) and false otherwise */
  public boolean isSetProcessorIdentifier() {
    return this.processorIdentifier != null;
  }

  public void setProcessorIdentifierIsSet(boolean value) {
    if (!value) {
      this.processorIdentifier = null;
    }
  }

  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case DB_NAME:
      if (value == null) {
        unsetDbName();
      } else {
        setDbName((java.lang.String)value);
      }
      break;

    case TBL_NAMES:
      if (value == null) {
        unsetTblNames();
      } else {
        setTblNames((java.util.List<java.lang.String>)value);
      }
      break;

    case CAPABILITIES:
      if (value == null) {
        unsetCapabilities();
      } else {
        setCapabilities((ClientCapabilities)value);
      }
      break;

    case CAT_NAME:
      if (value == null) {
        unsetCatName();
      } else {
        setCatName((java.lang.String)value);
      }
      break;

    case PROCESSOR_CAPABILITIES:
      if (value == null) {
        unsetProcessorCapabilities();
      } else {
        setProcessorCapabilities((java.util.List<java.lang.String>)value);
      }
      break;

    case PROCESSOR_IDENTIFIER:
      if (value == null) {
        unsetProcessorIdentifier();
      } else {
        setProcessorIdentifier((java.lang.String)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case DB_NAME:
      return getDbName();

    case TBL_NAMES:
      return getTblNames();

    case CAPABILITIES:
      return getCapabilities();

    case CAT_NAME:
      return getCatName();

    case PROCESSOR_CAPABILITIES:
      return getProcessorCapabilities();

    case PROCESSOR_IDENTIFIER:
      return getProcessorIdentifier();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case DB_NAME:
      return isSetDbName();
    case TBL_NAMES:
      return isSetTblNames();
    case CAPABILITIES:
      return isSetCapabilities();
    case CAT_NAME:
      return isSetCatName();
    case PROCESSOR_CAPABILITIES:
      return isSetProcessorCapabilities();
    case PROCESSOR_IDENTIFIER:
      return isSetProcessorIdentifier();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null)
      return false;
    if (that instanceof GetTablesRequest)
      return this.equals((GetTablesRequest)that);
    return false;
  }

  public boolean equals(GetTablesRequest that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_dbName = true && this.isSetDbName();
    boolean that_present_dbName = true && that.isSetDbName();
    if (this_present_dbName || that_present_dbName) {
      if (!(this_present_dbName && that_present_dbName))
        return false;
      if (!this.dbName.equals(that.dbName))
        return false;
    }

    boolean this_present_tblNames = true && this.isSetTblNames();
    boolean that_present_tblNames = true && that.isSetTblNames();
    if (this_present_tblNames || that_present_tblNames) {
      if (!(this_present_tblNames && that_present_tblNames))
        return false;
      if (!this.tblNames.equals(that.tblNames))
        return false;
    }

    boolean this_present_capabilities = true && this.isSetCapabilities();
    boolean that_present_capabilities = true && that.isSetCapabilities();
    if (this_present_capabilities || that_present_capabilities) {
      if (!(this_present_capabilities && that_present_capabilities))
        return false;
      if (!this.capabilities.equals(that.capabilities))
        return false;
    }

    boolean this_present_catName = true && this.isSetCatName();
    boolean that_present_catName = true && that.isSetCatName();
    if (this_present_catName || that_present_catName) {
      if (!(this_present_catName && that_present_catName))
        return false;
      if (!this.catName.equals(that.catName))
        return false;
    }

    boolean this_present_processorCapabilities = true && this.isSetProcessorCapabilities();
    boolean that_present_processorCapabilities = true && that.isSetProcessorCapabilities();
    if (this_present_processorCapabilities || that_present_processorCapabilities) {
      if (!(this_present_processorCapabilities && that_present_processorCapabilities))
        return false;
      if (!this.processorCapabilities.equals(that.processorCapabilities))
        return false;
    }

    boolean this_present_processorIdentifier = true && this.isSetProcessorIdentifier();
    boolean that_present_processorIdentifier = true && that.isSetProcessorIdentifier();
    if (this_present_processorIdentifier || that_present_processorIdentifier) {
      if (!(this_present_processorIdentifier && that_present_processorIdentifier))
        return false;
      if (!this.processorIdentifier.equals(that.processorIdentifier))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetDbName()) ? 131071 : 524287);
    if (isSetDbName())
      hashCode = hashCode * 8191 + dbName.hashCode();

    hashCode = hashCode * 8191 + ((isSetTblNames()) ? 131071 : 524287);
    if (isSetTblNames())
      hashCode = hashCode * 8191 + tblNames.hashCode();

    hashCode = hashCode * 8191 + ((isSetCapabilities()) ? 131071 : 524287);
    if (isSetCapabilities())
      hashCode = hashCode * 8191 + capabilities.hashCode();

    hashCode = hashCode * 8191 + ((isSetCatName()) ? 131071 : 524287);
    if (isSetCatName())
      hashCode = hashCode * 8191 + catName.hashCode();

    hashCode = hashCode * 8191 + ((isSetProcessorCapabilities()) ? 131071 : 524287);
    if (isSetProcessorCapabilities())
      hashCode = hashCode * 8191 + processorCapabilities.hashCode();

    hashCode = hashCode * 8191 + ((isSetProcessorIdentifier()) ? 131071 : 524287);
    if (isSetProcessorIdentifier())
      hashCode = hashCode * 8191 + processorIdentifier.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(GetTablesRequest other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.valueOf(isSetDbName()).compareTo(other.isSetDbName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetDbName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.dbName, other.dbName);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetTblNames()).compareTo(other.isSetTblNames());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetTblNames()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.tblNames, other.tblNames);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetCapabilities()).compareTo(other.isSetCapabilities());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetCapabilities()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.capabilities, other.capabilities);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetCatName()).compareTo(other.isSetCatName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetCatName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.catName, other.catName);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetProcessorCapabilities()).compareTo(other.isSetProcessorCapabilities());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetProcessorCapabilities()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.processorCapabilities, other.processorCapabilities);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetProcessorIdentifier()).compareTo(other.isSetProcessorIdentifier());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetProcessorIdentifier()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.processorIdentifier, other.processorIdentifier);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  @org.apache.thrift.annotation.Nullable
  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    scheme(iprot).read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    scheme(oprot).write(oprot, this);
  }

  @Override
  public java.lang.String toString() {
    java.lang.StringBuilder sb = new java.lang.StringBuilder("GetTablesRequest(");
    boolean first = true;

    sb.append("dbName:");
    if (this.dbName == null) {
      sb.append("null");
    } else {
      sb.append(this.dbName);
    }
    first = false;
    if (isSetTblNames()) {
      if (!first) sb.append(", ");
      sb.append("tblNames:");
      if (this.tblNames == null) {
        sb.append("null");
      } else {
        sb.append(this.tblNames);
      }
      first = false;
    }
    if (isSetCapabilities()) {
      if (!first) sb.append(", ");
      sb.append("capabilities:");
      if (this.capabilities == null) {
        sb.append("null");
      } else {
        sb.append(this.capabilities);
      }
      first = false;
    }
    if (isSetCatName()) {
      if (!first) sb.append(", ");
      sb.append("catName:");
      if (this.catName == null) {
        sb.append("null");
      } else {
        sb.append(this.catName);
      }
      first = false;
    }
    if (isSetProcessorCapabilities()) {
      if (!first) sb.append(", ");
      sb.append("processorCapabilities:");
      if (this.processorCapabilities == null) {
        sb.append("null");
      } else {
        sb.append(this.processorCapabilities);
      }
      first = false;
    }
    if (isSetProcessorIdentifier()) {
      if (!first) sb.append(", ");
      sb.append("processorIdentifier:");
      if (this.processorIdentifier == null) {
        sb.append("null");
      } else {
        sb.append(this.processorIdentifier);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!isSetDbName()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'dbName' is unset! Struct:" + toString());
    }

    // check for sub-struct validity
    if (capabilities != null) {
      capabilities.validate();
    }
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, java.lang.ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class GetTablesRequestStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public GetTablesRequestStandardScheme getScheme() {
      return new GetTablesRequestStandardScheme();
    }
  }

  private static class GetTablesRequestStandardScheme extends org.apache.thrift.scheme.StandardScheme<GetTablesRequest> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, GetTablesRequest struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // DB_NAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.dbName = iprot.readString();
              struct.setDbNameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // TBL_NAMES
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list952 = iprot.readListBegin();
                struct.tblNames = new java.util.ArrayList<java.lang.String>(_list952.size);
                @org.apache.thrift.annotation.Nullable java.lang.String _elem953;
                for (int _i954 = 0; _i954 < _list952.size; ++_i954)
                {
                  _elem953 = iprot.readString();
                  struct.tblNames.add(_elem953);
                }
                iprot.readListEnd();
              }
              struct.setTblNamesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // CAPABILITIES
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.capabilities = new ClientCapabilities();
              struct.capabilities.read(iprot);
              struct.setCapabilitiesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 4: // CAT_NAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.catName = iprot.readString();
              struct.setCatNameIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 5: // PROCESSOR_CAPABILITIES
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list955 = iprot.readListBegin();
                struct.processorCapabilities = new java.util.ArrayList<java.lang.String>(_list955.size);
                @org.apache.thrift.annotation.Nullable java.lang.String _elem956;
                for (int _i957 = 0; _i957 < _list955.size; ++_i957)
                {
                  _elem956 = iprot.readString();
                  struct.processorCapabilities.add(_elem956);
                }
                iprot.readListEnd();
              }
              struct.setProcessorCapabilitiesIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 6: // PROCESSOR_IDENTIFIER
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.processorIdentifier = iprot.readString();
              struct.setProcessorIdentifierIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, GetTablesRequest struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.dbName != null) {
        oprot.writeFieldBegin(DB_NAME_FIELD_DESC);
        oprot.writeString(struct.dbName);
        oprot.writeFieldEnd();
      }
      if (struct.tblNames != null) {
        if (struct.isSetTblNames()) {
          oprot.writeFieldBegin(TBL_NAMES_FIELD_DESC);
          {
            oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, struct.tblNames.size()));
            for (java.lang.String _iter958 : struct.tblNames)
            {
              oprot.writeString(_iter958);
            }
            oprot.writeListEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      if (struct.capabilities != null) {
        if (struct.isSetCapabilities()) {
          oprot.writeFieldBegin(CAPABILITIES_FIELD_DESC);
          struct.capabilities.write(oprot);
          oprot.writeFieldEnd();
        }
      }
      if (struct.catName != null) {
        if (struct.isSetCatName()) {
          oprot.writeFieldBegin(CAT_NAME_FIELD_DESC);
          oprot.writeString(struct.catName);
          oprot.writeFieldEnd();
        }
      }
      if (struct.processorCapabilities != null) {
        if (struct.isSetProcessorCapabilities()) {
          oprot.writeFieldBegin(PROCESSOR_CAPABILITIES_FIELD_DESC);
          {
            oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, struct.processorCapabilities.size()));
            for (java.lang.String _iter959 : struct.processorCapabilities)
            {
              oprot.writeString(_iter959);
            }
            oprot.writeListEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      if (struct.processorIdentifier != null) {
        if (struct.isSetProcessorIdentifier()) {
          oprot.writeFieldBegin(PROCESSOR_IDENTIFIER_FIELD_DESC);
          oprot.writeString(struct.processorIdentifier);
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class GetTablesRequestTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public GetTablesRequestTupleScheme getScheme() {
      return new GetTablesRequestTupleScheme();
    }
  }

  private static class GetTablesRequestTupleScheme extends org.apache.thrift.scheme.TupleScheme<GetTablesRequest> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, GetTablesRequest struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      oprot.writeString(struct.dbName);
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetTblNames()) {
        optionals.set(0);
      }
      if (struct.isSetCapabilities()) {
        optionals.set(1);
      }
      if (struct.isSetCatName()) {
        optionals.set(2);
      }
      if (struct.isSetProcessorCapabilities()) {
        optionals.set(3);
      }
      if (struct.isSetProcessorIdentifier()) {
        optionals.set(4);
      }
      oprot.writeBitSet(optionals, 5);
      if (struct.isSetTblNames()) {
        {
          oprot.writeI32(struct.tblNames.size());
          for (java.lang.String _iter960 : struct.tblNames)
          {
            oprot.writeString(_iter960);
          }
        }
      }
      if (struct.isSetCapabilities()) {
        struct.capabilities.write(oprot);
      }
      if (struct.isSetCatName()) {
        oprot.writeString(struct.catName);
      }
      if (struct.isSetProcessorCapabilities()) {
        {
          oprot.writeI32(struct.processorCapabilities.size());
          for (java.lang.String _iter961 : struct.processorCapabilities)
          {
            oprot.writeString(_iter961);
          }
        }
      }
      if (struct.isSetProcessorIdentifier()) {
        oprot.writeString(struct.processorIdentifier);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, GetTablesRequest struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      struct.dbName = iprot.readString();
      struct.setDbNameIsSet(true);
      java.util.BitSet incoming = iprot.readBitSet(5);
      if (incoming.get(0)) {
        {
          org.apache.thrift.protocol.TList _list962 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.tblNames = new java.util.ArrayList<java.lang.String>(_list962.size);
          @org.apache.thrift.annotation.Nullable java.lang.String _elem963;
          for (int _i964 = 0; _i964 < _list962.size; ++_i964)
          {
            _elem963 = iprot.readString();
            struct.tblNames.add(_elem963);
          }
        }
        struct.setTblNamesIsSet(true);
      }
      if (incoming.get(1)) {
        struct.capabilities = new ClientCapabilities();
        struct.capabilities.read(iprot);
        struct.setCapabilitiesIsSet(true);
      }
      if (incoming.get(2)) {
        struct.catName = iprot.readString();
        struct.setCatNameIsSet(true);
      }
      if (incoming.get(3)) {
        {
          org.apache.thrift.protocol.TList _list965 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRING, iprot.readI32());
          struct.processorCapabilities = new java.util.ArrayList<java.lang.String>(_list965.size);
          @org.apache.thrift.annotation.Nullable java.lang.String _elem966;
          for (int _i967 = 0; _i967 < _list965.size; ++_i967)
          {
            _elem966 = iprot.readString();
            struct.processorCapabilities.add(_elem966);
          }
        }
        struct.setProcessorCapabilitiesIsSet(true);
      }
      if (incoming.get(4)) {
        struct.processorIdentifier = iprot.readString();
        struct.setProcessorIdentifierIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

