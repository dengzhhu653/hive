/**
 * Autogenerated by Thrift Compiler (0.13.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.hadoop.hive.metastore.api;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.13.0)")
@org.apache.hadoop.classification.InterfaceAudience.Public @org.apache.hadoop.classification.InterfaceStability.Stable public class NotificationEventsCountResponse implements org.apache.thrift.TBase<NotificationEventsCountResponse, NotificationEventsCountResponse._Fields>, java.io.Serializable, Cloneable, Comparable<NotificationEventsCountResponse> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("NotificationEventsCountResponse");

  private static final org.apache.thrift.protocol.TField EVENTS_COUNT_FIELD_DESC = new org.apache.thrift.protocol.TField("eventsCount", org.apache.thrift.protocol.TType.I64, (short)1);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new NotificationEventsCountResponseStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new NotificationEventsCountResponseTupleSchemeFactory();

  private long eventsCount; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    EVENTS_COUNT((short)1, "eventsCount");

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
        case 1: // EVENTS_COUNT
          return EVENTS_COUNT;
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
  private static final int __EVENTSCOUNT_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  public static final java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    java.util.Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new java.util.EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.EVENTS_COUNT, new org.apache.thrift.meta_data.FieldMetaData("eventsCount", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(NotificationEventsCountResponse.class, metaDataMap);
  }

  public NotificationEventsCountResponse() {
  }

  public NotificationEventsCountResponse(
    long eventsCount)
  {
    this();
    this.eventsCount = eventsCount;
    setEventsCountIsSet(true);
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public NotificationEventsCountResponse(NotificationEventsCountResponse other) {
    __isset_bitfield = other.__isset_bitfield;
    this.eventsCount = other.eventsCount;
  }

  public NotificationEventsCountResponse deepCopy() {
    return new NotificationEventsCountResponse(this);
  }

  @Override
  public void clear() {
    setEventsCountIsSet(false);
    this.eventsCount = 0;
  }

  public long getEventsCount() {
    return this.eventsCount;
  }

  public void setEventsCount(long eventsCount) {
    this.eventsCount = eventsCount;
    setEventsCountIsSet(true);
  }

  public void unsetEventsCount() {
    __isset_bitfield = org.apache.thrift.EncodingUtils.clearBit(__isset_bitfield, __EVENTSCOUNT_ISSET_ID);
  }

  /** Returns true if field eventsCount is set (has been assigned a value) and false otherwise */
  public boolean isSetEventsCount() {
    return org.apache.thrift.EncodingUtils.testBit(__isset_bitfield, __EVENTSCOUNT_ISSET_ID);
  }

  public void setEventsCountIsSet(boolean value) {
    __isset_bitfield = org.apache.thrift.EncodingUtils.setBit(__isset_bitfield, __EVENTSCOUNT_ISSET_ID, value);
  }

  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case EVENTS_COUNT:
      if (value == null) {
        unsetEventsCount();
      } else {
        setEventsCount((java.lang.Long)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case EVENTS_COUNT:
      return getEventsCount();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case EVENTS_COUNT:
      return isSetEventsCount();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null)
      return false;
    if (that instanceof NotificationEventsCountResponse)
      return this.equals((NotificationEventsCountResponse)that);
    return false;
  }

  public boolean equals(NotificationEventsCountResponse that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_eventsCount = true;
    boolean that_present_eventsCount = true;
    if (this_present_eventsCount || that_present_eventsCount) {
      if (!(this_present_eventsCount && that_present_eventsCount))
        return false;
      if (this.eventsCount != that.eventsCount)
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + org.apache.thrift.TBaseHelper.hashCode(eventsCount);

    return hashCode;
  }

  @Override
  public int compareTo(NotificationEventsCountResponse other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.valueOf(isSetEventsCount()).compareTo(other.isSetEventsCount());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetEventsCount()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.eventsCount, other.eventsCount);
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
    java.lang.StringBuilder sb = new java.lang.StringBuilder("NotificationEventsCountResponse(");
    boolean first = true;

    sb.append("eventsCount:");
    sb.append(this.eventsCount);
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!isSetEventsCount()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'eventsCount' is unset! Struct:" + toString());
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
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class NotificationEventsCountResponseStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public NotificationEventsCountResponseStandardScheme getScheme() {
      return new NotificationEventsCountResponseStandardScheme();
    }
  }

  private static class NotificationEventsCountResponseStandardScheme extends org.apache.thrift.scheme.StandardScheme<NotificationEventsCountResponse> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, NotificationEventsCountResponse struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // EVENTS_COUNT
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.eventsCount = iprot.readI64();
              struct.setEventsCountIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, NotificationEventsCountResponse struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(EVENTS_COUNT_FIELD_DESC);
      oprot.writeI64(struct.eventsCount);
      oprot.writeFieldEnd();
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class NotificationEventsCountResponseTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public NotificationEventsCountResponseTupleScheme getScheme() {
      return new NotificationEventsCountResponseTupleScheme();
    }
  }

  private static class NotificationEventsCountResponseTupleScheme extends org.apache.thrift.scheme.TupleScheme<NotificationEventsCountResponse> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, NotificationEventsCountResponse struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      oprot.writeI64(struct.eventsCount);
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, NotificationEventsCountResponse struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      struct.eventsCount = iprot.readI64();
      struct.setEventsCountIsSet(true);
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

