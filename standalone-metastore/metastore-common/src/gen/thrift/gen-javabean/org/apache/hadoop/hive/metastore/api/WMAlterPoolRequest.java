/**
 * Autogenerated by Thrift Compiler (0.13.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.hadoop.hive.metastore.api;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.13.0)")
@org.apache.hadoop.classification.InterfaceAudience.Public @org.apache.hadoop.classification.InterfaceStability.Stable public class WMAlterPoolRequest implements org.apache.thrift.TBase<WMAlterPoolRequest, WMAlterPoolRequest._Fields>, java.io.Serializable, Cloneable, Comparable<WMAlterPoolRequest> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("WMAlterPoolRequest");

  private static final org.apache.thrift.protocol.TField POOL_FIELD_DESC = new org.apache.thrift.protocol.TField("pool", org.apache.thrift.protocol.TType.STRUCT, (short)1);
  private static final org.apache.thrift.protocol.TField POOL_PATH_FIELD_DESC = new org.apache.thrift.protocol.TField("poolPath", org.apache.thrift.protocol.TType.STRING, (short)2);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new WMAlterPoolRequestStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new WMAlterPoolRequestTupleSchemeFactory();

  private @org.apache.thrift.annotation.Nullable WMNullablePool pool; // optional
  private @org.apache.thrift.annotation.Nullable java.lang.String poolPath; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    POOL((short)1, "pool"),
    POOL_PATH((short)2, "poolPath");

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
        case 1: // POOL
          return POOL;
        case 2: // POOL_PATH
          return POOL_PATH;
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
  private static final _Fields optionals[] = {_Fields.POOL,_Fields.POOL_PATH};
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.POOL, new org.apache.thrift.meta_data.FieldMetaData("pool", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, WMNullablePool.class)));
    tmpMap.put(_Fields.POOL_PATH, new org.apache.thrift.meta_data.FieldMetaData("poolPath", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(WMAlterPoolRequest.class, metaDataMap);
  }

  public WMAlterPoolRequest() {
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public WMAlterPoolRequest(WMAlterPoolRequest other) {
    if (other.isSetPool()) {
      this.pool = new WMNullablePool(other.pool);
    }
    if (other.isSetPoolPath()) {
      this.poolPath = other.poolPath;
    }
  }

  public WMAlterPoolRequest deepCopy() {
    return new WMAlterPoolRequest(this);
  }

  @Override
  public void clear() {
    this.pool = null;
    this.poolPath = null;
  }

  @org.apache.thrift.annotation.Nullable
  public WMNullablePool getPool() {
    return this.pool;
  }

  public void setPool(@org.apache.thrift.annotation.Nullable WMNullablePool pool) {
    this.pool = pool;
  }

  public void unsetPool() {
    this.pool = null;
  }

  /** Returns true if field pool is set (has been assigned a value) and false otherwise */
  public boolean isSetPool() {
    return this.pool != null;
  }

  public void setPoolIsSet(boolean value) {
    if (!value) {
      this.pool = null;
    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.String getPoolPath() {
    return this.poolPath;
  }

  public void setPoolPath(@org.apache.thrift.annotation.Nullable java.lang.String poolPath) {
    this.poolPath = poolPath;
  }

  public void unsetPoolPath() {
    this.poolPath = null;
  }

  /** Returns true if field poolPath is set (has been assigned a value) and false otherwise */
  public boolean isSetPoolPath() {
    return this.poolPath != null;
  }

  public void setPoolPathIsSet(boolean value) {
    if (!value) {
      this.poolPath = null;
    }
  }

  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case POOL:
      if (value == null) {
        unsetPool();
      } else {
        setPool((WMNullablePool)value);
      }
      break;

    case POOL_PATH:
      if (value == null) {
        unsetPoolPath();
      } else {
        setPoolPath((java.lang.String)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case POOL:
      return getPool();

    case POOL_PATH:
      return getPoolPath();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case POOL:
      return isSetPool();
    case POOL_PATH:
      return isSetPoolPath();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null)
      return false;
    if (that instanceof WMAlterPoolRequest)
      return this.equals((WMAlterPoolRequest)that);
    return false;
  }

  public boolean equals(WMAlterPoolRequest that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_pool = true && this.isSetPool();
    boolean that_present_pool = true && that.isSetPool();
    if (this_present_pool || that_present_pool) {
      if (!(this_present_pool && that_present_pool))
        return false;
      if (!this.pool.equals(that.pool))
        return false;
    }

    boolean this_present_poolPath = true && this.isSetPoolPath();
    boolean that_present_poolPath = true && that.isSetPoolPath();
    if (this_present_poolPath || that_present_poolPath) {
      if (!(this_present_poolPath && that_present_poolPath))
        return false;
      if (!this.poolPath.equals(that.poolPath))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetPool()) ? 131071 : 524287);
    if (isSetPool())
      hashCode = hashCode * 8191 + pool.hashCode();

    hashCode = hashCode * 8191 + ((isSetPoolPath()) ? 131071 : 524287);
    if (isSetPoolPath())
      hashCode = hashCode * 8191 + poolPath.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(WMAlterPoolRequest other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.valueOf(isSetPool()).compareTo(other.isSetPool());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetPool()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.pool, other.pool);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = java.lang.Boolean.valueOf(isSetPoolPath()).compareTo(other.isSetPoolPath());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetPoolPath()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.poolPath, other.poolPath);
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
    java.lang.StringBuilder sb = new java.lang.StringBuilder("WMAlterPoolRequest(");
    boolean first = true;

    if (isSetPool()) {
      sb.append("pool:");
      if (this.pool == null) {
        sb.append("null");
      } else {
        sb.append(this.pool);
      }
      first = false;
    }
    if (isSetPoolPath()) {
      if (!first) sb.append(", ");
      sb.append("poolPath:");
      if (this.poolPath == null) {
        sb.append("null");
      } else {
        sb.append(this.poolPath);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
    if (pool != null) {
      pool.validate();
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

  private static class WMAlterPoolRequestStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public WMAlterPoolRequestStandardScheme getScheme() {
      return new WMAlterPoolRequestStandardScheme();
    }
  }

  private static class WMAlterPoolRequestStandardScheme extends org.apache.thrift.scheme.StandardScheme<WMAlterPoolRequest> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, WMAlterPoolRequest struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // POOL
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.pool = new WMNullablePool();
              struct.pool.read(iprot);
              struct.setPoolIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // POOL_PATH
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.poolPath = iprot.readString();
              struct.setPoolPathIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, WMAlterPoolRequest struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.pool != null) {
        if (struct.isSetPool()) {
          oprot.writeFieldBegin(POOL_FIELD_DESC);
          struct.pool.write(oprot);
          oprot.writeFieldEnd();
        }
      }
      if (struct.poolPath != null) {
        if (struct.isSetPoolPath()) {
          oprot.writeFieldBegin(POOL_PATH_FIELD_DESC);
          oprot.writeString(struct.poolPath);
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class WMAlterPoolRequestTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public WMAlterPoolRequestTupleScheme getScheme() {
      return new WMAlterPoolRequestTupleScheme();
    }
  }

  private static class WMAlterPoolRequestTupleScheme extends org.apache.thrift.scheme.TupleScheme<WMAlterPoolRequest> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, WMAlterPoolRequest struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet optionals = new java.util.BitSet();
      if (struct.isSetPool()) {
        optionals.set(0);
      }
      if (struct.isSetPoolPath()) {
        optionals.set(1);
      }
      oprot.writeBitSet(optionals, 2);
      if (struct.isSetPool()) {
        struct.pool.write(oprot);
      }
      if (struct.isSetPoolPath()) {
        oprot.writeString(struct.poolPath);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, WMAlterPoolRequest struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      java.util.BitSet incoming = iprot.readBitSet(2);
      if (incoming.get(0)) {
        struct.pool = new WMNullablePool();
        struct.pool.read(iprot);
        struct.setPoolIsSet(true);
      }
      if (incoming.get(1)) {
        struct.poolPath = iprot.readString();
        struct.setPoolPathIsSet(true);
      }
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

