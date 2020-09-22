/**
 * Autogenerated by Thrift Compiler (0.13.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.hadoop.hive.metastore.api;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.13.0)")
@org.apache.hadoop.classification.InterfaceAudience.Public @org.apache.hadoop.classification.InterfaceStability.Stable public class NotNullConstraintsResponse implements org.apache.thrift.TBase<NotNullConstraintsResponse, NotNullConstraintsResponse._Fields>, java.io.Serializable, Cloneable, Comparable<NotNullConstraintsResponse> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("NotNullConstraintsResponse");

  private static final org.apache.thrift.protocol.TField NOT_NULL_CONSTRAINTS_FIELD_DESC = new org.apache.thrift.protocol.TField("notNullConstraints", org.apache.thrift.protocol.TType.LIST, (short)1);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new NotNullConstraintsResponseStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new NotNullConstraintsResponseTupleSchemeFactory();

  private @org.apache.thrift.annotation.Nullable java.util.List<SQLNotNullConstraint> notNullConstraints; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    NOT_NULL_CONSTRAINTS((short)1, "notNullConstraints");

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
        case 1: // NOT_NULL_CONSTRAINTS
          return NOT_NULL_CONSTRAINTS;
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
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.NOT_NULL_CONSTRAINTS, new org.apache.thrift.meta_data.FieldMetaData("notNullConstraints", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, SQLNotNullConstraint.class))));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(NotNullConstraintsResponse.class, metaDataMap);
  }

  public NotNullConstraintsResponse() {
  }

  public NotNullConstraintsResponse(
    java.util.List<SQLNotNullConstraint> notNullConstraints)
  {
    this();
    this.notNullConstraints = notNullConstraints;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public NotNullConstraintsResponse(NotNullConstraintsResponse other) {
    if (other.isSetNotNullConstraints()) {
      java.util.List<SQLNotNullConstraint> __this__notNullConstraints = new java.util.ArrayList<SQLNotNullConstraint>(other.notNullConstraints.size());
      for (SQLNotNullConstraint other_element : other.notNullConstraints) {
        __this__notNullConstraints.add(new SQLNotNullConstraint(other_element));
      }
      this.notNullConstraints = __this__notNullConstraints;
    }
  }

  public NotNullConstraintsResponse deepCopy() {
    return new NotNullConstraintsResponse(this);
  }

  @Override
  public void clear() {
    this.notNullConstraints = null;
  }

  public int getNotNullConstraintsSize() {
    return (this.notNullConstraints == null) ? 0 : this.notNullConstraints.size();
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Iterator<SQLNotNullConstraint> getNotNullConstraintsIterator() {
    return (this.notNullConstraints == null) ? null : this.notNullConstraints.iterator();
  }

  public void addToNotNullConstraints(SQLNotNullConstraint elem) {
    if (this.notNullConstraints == null) {
      this.notNullConstraints = new java.util.ArrayList<SQLNotNullConstraint>();
    }
    this.notNullConstraints.add(elem);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.List<SQLNotNullConstraint> getNotNullConstraints() {
    return this.notNullConstraints;
  }

  public void setNotNullConstraints(@org.apache.thrift.annotation.Nullable java.util.List<SQLNotNullConstraint> notNullConstraints) {
    this.notNullConstraints = notNullConstraints;
  }

  public void unsetNotNullConstraints() {
    this.notNullConstraints = null;
  }

  /** Returns true if field notNullConstraints is set (has been assigned a value) and false otherwise */
  public boolean isSetNotNullConstraints() {
    return this.notNullConstraints != null;
  }

  public void setNotNullConstraintsIsSet(boolean value) {
    if (!value) {
      this.notNullConstraints = null;
    }
  }

  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case NOT_NULL_CONSTRAINTS:
      if (value == null) {
        unsetNotNullConstraints();
      } else {
        setNotNullConstraints((java.util.List<SQLNotNullConstraint>)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case NOT_NULL_CONSTRAINTS:
      return getNotNullConstraints();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case NOT_NULL_CONSTRAINTS:
      return isSetNotNullConstraints();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null)
      return false;
    if (that instanceof NotNullConstraintsResponse)
      return this.equals((NotNullConstraintsResponse)that);
    return false;
  }

  public boolean equals(NotNullConstraintsResponse that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_notNullConstraints = true && this.isSetNotNullConstraints();
    boolean that_present_notNullConstraints = true && that.isSetNotNullConstraints();
    if (this_present_notNullConstraints || that_present_notNullConstraints) {
      if (!(this_present_notNullConstraints && that_present_notNullConstraints))
        return false;
      if (!this.notNullConstraints.equals(that.notNullConstraints))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetNotNullConstraints()) ? 131071 : 524287);
    if (isSetNotNullConstraints())
      hashCode = hashCode * 8191 + notNullConstraints.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(NotNullConstraintsResponse other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.valueOf(isSetNotNullConstraints()).compareTo(other.isSetNotNullConstraints());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetNotNullConstraints()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.notNullConstraints, other.notNullConstraints);
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
    java.lang.StringBuilder sb = new java.lang.StringBuilder("NotNullConstraintsResponse(");
    boolean first = true;

    sb.append("notNullConstraints:");
    if (this.notNullConstraints == null) {
      sb.append("null");
    } else {
      sb.append(this.notNullConstraints);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!isSetNotNullConstraints()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'notNullConstraints' is unset! Struct:" + toString());
    }

    // check for sub-struct validity
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

  private static class NotNullConstraintsResponseStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public NotNullConstraintsResponseStandardScheme getScheme() {
      return new NotNullConstraintsResponseStandardScheme();
    }
  }

  private static class NotNullConstraintsResponseStandardScheme extends org.apache.thrift.scheme.StandardScheme<NotNullConstraintsResponse> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, NotNullConstraintsResponse struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // NOT_NULL_CONSTRAINTS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list376 = iprot.readListBegin();
                struct.notNullConstraints = new java.util.ArrayList<SQLNotNullConstraint>(_list376.size);
                @org.apache.thrift.annotation.Nullable SQLNotNullConstraint _elem377;
                for (int _i378 = 0; _i378 < _list376.size; ++_i378)
                {
                  _elem377 = new SQLNotNullConstraint();
                  _elem377.read(iprot);
                  struct.notNullConstraints.add(_elem377);
                }
                iprot.readListEnd();
              }
              struct.setNotNullConstraintsIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, NotNullConstraintsResponse struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.notNullConstraints != null) {
        oprot.writeFieldBegin(NOT_NULL_CONSTRAINTS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.notNullConstraints.size()));
          for (SQLNotNullConstraint _iter379 : struct.notNullConstraints)
          {
            _iter379.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class NotNullConstraintsResponseTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public NotNullConstraintsResponseTupleScheme getScheme() {
      return new NotNullConstraintsResponseTupleScheme();
    }
  }

  private static class NotNullConstraintsResponseTupleScheme extends org.apache.thrift.scheme.TupleScheme<NotNullConstraintsResponse> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, NotNullConstraintsResponse struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      {
        oprot.writeI32(struct.notNullConstraints.size());
        for (SQLNotNullConstraint _iter380 : struct.notNullConstraints)
        {
          _iter380.write(oprot);
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, NotNullConstraintsResponse struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      {
        org.apache.thrift.protocol.TList _list381 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
        struct.notNullConstraints = new java.util.ArrayList<SQLNotNullConstraint>(_list381.size);
        @org.apache.thrift.annotation.Nullable SQLNotNullConstraint _elem382;
        for (int _i383 = 0; _i383 < _list381.size; ++_i383)
        {
          _elem382 = new SQLNotNullConstraint();
          _elem382.read(iprot);
          struct.notNullConstraints.add(_elem382);
        }
      }
      struct.setNotNullConstraintsIsSet(true);
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

