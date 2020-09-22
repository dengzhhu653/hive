/**
 * Autogenerated by Thrift Compiler (0.13.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package org.apache.hadoop.hive.metastore.api;

@SuppressWarnings({"cast", "rawtypes", "serial", "unchecked", "unused"})
@javax.annotation.Generated(value = "Autogenerated by Thrift Compiler (0.13.0)")
@org.apache.hadoop.classification.InterfaceAudience.Public @org.apache.hadoop.classification.InterfaceStability.Stable public class NotificationEventResponse implements org.apache.thrift.TBase<NotificationEventResponse, NotificationEventResponse._Fields>, java.io.Serializable, Cloneable, Comparable<NotificationEventResponse> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("NotificationEventResponse");

  private static final org.apache.thrift.protocol.TField EVENTS_FIELD_DESC = new org.apache.thrift.protocol.TField("events", org.apache.thrift.protocol.TType.LIST, (short)1);

  private static final org.apache.thrift.scheme.SchemeFactory STANDARD_SCHEME_FACTORY = new NotificationEventResponseStandardSchemeFactory();
  private static final org.apache.thrift.scheme.SchemeFactory TUPLE_SCHEME_FACTORY = new NotificationEventResponseTupleSchemeFactory();

  private @org.apache.thrift.annotation.Nullable java.util.List<NotificationEvent> events; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    EVENTS((short)1, "events");

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
        case 1: // EVENTS
          return EVENTS;
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
    tmpMap.put(_Fields.EVENTS, new org.apache.thrift.meta_data.FieldMetaData("events", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, NotificationEvent.class))));
    metaDataMap = java.util.Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(NotificationEventResponse.class, metaDataMap);
  }

  public NotificationEventResponse() {
  }

  public NotificationEventResponse(
    java.util.List<NotificationEvent> events)
  {
    this();
    this.events = events;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public NotificationEventResponse(NotificationEventResponse other) {
    if (other.isSetEvents()) {
      java.util.List<NotificationEvent> __this__events = new java.util.ArrayList<NotificationEvent>(other.events.size());
      for (NotificationEvent other_element : other.events) {
        __this__events.add(new NotificationEvent(other_element));
      }
      this.events = __this__events;
    }
  }

  public NotificationEventResponse deepCopy() {
    return new NotificationEventResponse(this);
  }

  @Override
  public void clear() {
    this.events = null;
  }

  public int getEventsSize() {
    return (this.events == null) ? 0 : this.events.size();
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.Iterator<NotificationEvent> getEventsIterator() {
    return (this.events == null) ? null : this.events.iterator();
  }

  public void addToEvents(NotificationEvent elem) {
    if (this.events == null) {
      this.events = new java.util.ArrayList<NotificationEvent>();
    }
    this.events.add(elem);
  }

  @org.apache.thrift.annotation.Nullable
  public java.util.List<NotificationEvent> getEvents() {
    return this.events;
  }

  public void setEvents(@org.apache.thrift.annotation.Nullable java.util.List<NotificationEvent> events) {
    this.events = events;
  }

  public void unsetEvents() {
    this.events = null;
  }

  /** Returns true if field events is set (has been assigned a value) and false otherwise */
  public boolean isSetEvents() {
    return this.events != null;
  }

  public void setEventsIsSet(boolean value) {
    if (!value) {
      this.events = null;
    }
  }

  public void setFieldValue(_Fields field, @org.apache.thrift.annotation.Nullable java.lang.Object value) {
    switch (field) {
    case EVENTS:
      if (value == null) {
        unsetEvents();
      } else {
        setEvents((java.util.List<NotificationEvent>)value);
      }
      break;

    }
  }

  @org.apache.thrift.annotation.Nullable
  public java.lang.Object getFieldValue(_Fields field) {
    switch (field) {
    case EVENTS:
      return getEvents();

    }
    throw new java.lang.IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new java.lang.IllegalArgumentException();
    }

    switch (field) {
    case EVENTS:
      return isSetEvents();
    }
    throw new java.lang.IllegalStateException();
  }

  @Override
  public boolean equals(java.lang.Object that) {
    if (that == null)
      return false;
    if (that instanceof NotificationEventResponse)
      return this.equals((NotificationEventResponse)that);
    return false;
  }

  public boolean equals(NotificationEventResponse that) {
    if (that == null)
      return false;
    if (this == that)
      return true;

    boolean this_present_events = true && this.isSetEvents();
    boolean that_present_events = true && that.isSetEvents();
    if (this_present_events || that_present_events) {
      if (!(this_present_events && that_present_events))
        return false;
      if (!this.events.equals(that.events))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int hashCode = 1;

    hashCode = hashCode * 8191 + ((isSetEvents()) ? 131071 : 524287);
    if (isSetEvents())
      hashCode = hashCode * 8191 + events.hashCode();

    return hashCode;
  }

  @Override
  public int compareTo(NotificationEventResponse other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = java.lang.Boolean.valueOf(isSetEvents()).compareTo(other.isSetEvents());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetEvents()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.events, other.events);
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
    java.lang.StringBuilder sb = new java.lang.StringBuilder("NotificationEventResponse(");
    boolean first = true;

    sb.append("events:");
    if (this.events == null) {
      sb.append("null");
    } else {
      sb.append(this.events);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!isSetEvents()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'events' is unset! Struct:" + toString());
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

  private static class NotificationEventResponseStandardSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public NotificationEventResponseStandardScheme getScheme() {
      return new NotificationEventResponseStandardScheme();
    }
  }

  private static class NotificationEventResponseStandardScheme extends org.apache.thrift.scheme.StandardScheme<NotificationEventResponse> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, NotificationEventResponse struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // EVENTS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list796 = iprot.readListBegin();
                struct.events = new java.util.ArrayList<NotificationEvent>(_list796.size);
                @org.apache.thrift.annotation.Nullable NotificationEvent _elem797;
                for (int _i798 = 0; _i798 < _list796.size; ++_i798)
                {
                  _elem797 = new NotificationEvent();
                  _elem797.read(iprot);
                  struct.events.add(_elem797);
                }
                iprot.readListEnd();
              }
              struct.setEventsIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, NotificationEventResponse struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.events != null) {
        oprot.writeFieldBegin(EVENTS_FIELD_DESC);
        {
          oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, struct.events.size()));
          for (NotificationEvent _iter799 : struct.events)
          {
            _iter799.write(oprot);
          }
          oprot.writeListEnd();
        }
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class NotificationEventResponseTupleSchemeFactory implements org.apache.thrift.scheme.SchemeFactory {
    public NotificationEventResponseTupleScheme getScheme() {
      return new NotificationEventResponseTupleScheme();
    }
  }

  private static class NotificationEventResponseTupleScheme extends org.apache.thrift.scheme.TupleScheme<NotificationEventResponse> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, NotificationEventResponse struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol oprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      {
        oprot.writeI32(struct.events.size());
        for (NotificationEvent _iter800 : struct.events)
        {
          _iter800.write(oprot);
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, NotificationEventResponse struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TTupleProtocol iprot = (org.apache.thrift.protocol.TTupleProtocol) prot;
      {
        org.apache.thrift.protocol.TList _list801 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
        struct.events = new java.util.ArrayList<NotificationEvent>(_list801.size);
        @org.apache.thrift.annotation.Nullable NotificationEvent _elem802;
        for (int _i803 = 0; _i803 < _list801.size; ++_i803)
        {
          _elem802 = new NotificationEvent();
          _elem802.read(iprot);
          struct.events.add(_elem802);
        }
      }
      struct.setEventsIsSet(true);
    }
  }

  private static <S extends org.apache.thrift.scheme.IScheme> S scheme(org.apache.thrift.protocol.TProtocol proto) {
    return (org.apache.thrift.scheme.StandardScheme.class.equals(proto.getScheme()) ? STANDARD_SCHEME_FACTORY : TUPLE_SCHEME_FACTORY).getScheme();
  }
}

