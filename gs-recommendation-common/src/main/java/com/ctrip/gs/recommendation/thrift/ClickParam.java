package com.ctrip.gs.recommendation.thrift;

import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;

import com.twitter.scrooge.Option;
import com.twitter.scrooge.ThriftStruct;
import com.twitter.scrooge.ThriftStructCodec3;

public class ClickParam implements ThriftStruct {
  private static final TStruct STRUCT = new TStruct("ClickParam");
  private static final TField ItemIdField = new TField("itemId", TType.I32, (short) 1);
  final int itemId;
  private static final TField ItemNameField = new TField("itemName", TType.STRING, (short) 2);
  final String itemName;
  private static final TField TypeField = new TField("type", TType.I32, (short) 3);
  final Option<RecommendType> type;
  private static final TField UseridField = new TField("userid", TType.STRING, (short) 4);
  final String userid;

  public static class Builder {
    private int _itemId = 0;
    private Boolean _got_itemId = false;

    public Builder itemId(int value) {
      this._itemId = value;
      this._got_itemId = true;
      return this;
    }
    private String _itemName = null;
    private Boolean _got_itemName = false;

    public Builder itemName(String value) {
      this._itemName = value;
      this._got_itemName = true;
      return this;
    }
    private RecommendType _type = null;
    private Boolean _got_type = false;

    public Builder type(RecommendType value) {
      this._type = value;
      this._got_type = true;
      return this;
    }
    private String _userid = "1";
    private Boolean _got_userid = false;

    public Builder userid(String value) {
      this._userid = value;
      this._got_userid = true;
      return this;
    }

    public ClickParam build() {
      if (!_got_itemId)
      throw new IllegalStateException("Required field 'itemId' was not found for struct ClickParam");
      if (!_got_itemName)
      throw new IllegalStateException("Required field 'itemName' was not found for struct ClickParam");
      return new ClickParam(
        this._itemId,
        this._itemName,
      Option.make(this._got_type, this._type),
        this._userid    );
    }
  }

  public Builder copy() {
    Builder builder = new Builder();
    builder.itemId(this.itemId);
    builder.itemName(this.itemName);
    if (this.type.isDefined()) builder.type(this.type.get());
    builder.userid(this.userid);
    return builder;
  }

  public static ThriftStructCodec3<ClickParam> CODEC = new ThriftStructCodec3<ClickParam>() {
    public ClickParam decode(TProtocol _iprot) throws org.apache.thrift.TException {
      Builder builder = new Builder();
      int itemId = 0;
      String itemName = null;
      RecommendType type = null;
      String userid = "1";
      Boolean _done = false;
      _iprot.readStructBegin();
      while (!_done) {
        TField _field = _iprot.readFieldBegin();
        if (_field.type == TType.STOP) {
          _done = true;
        } else {
          switch (_field.id) {
            case 1: /* itemId */
              switch (_field.type) {
                case TType.I32:
                  Integer itemId_item;
                  itemId_item = _iprot.readI32();
                  itemId = itemId_item;
                  break;
                default:
                  TProtocolUtil.skip(_iprot, _field.type);
              }
              builder.itemId(itemId);
              break;
            case 2: /* itemName */
              switch (_field.type) {
                case TType.STRING:
                  String itemName_item;
                  itemName_item = _iprot.readString();
                  itemName = itemName_item;
                  break;
                default:
                  TProtocolUtil.skip(_iprot, _field.type);
              }
              builder.itemName(itemName);
              break;
            case 3: /* type */
              switch (_field.type) {
                case TType.I32:
                  RecommendType type_item;
                  type_item = RecommendType.findByValue(_iprot.readI32());
                  type = type_item;
                  break;
                default:
                  TProtocolUtil.skip(_iprot, _field.type);
              }
              builder.type(type);
              break;
            case 4: /* userid */
              switch (_field.type) {
                case TType.STRING:
                  String userid_item;
                  userid_item = _iprot.readString();
                  userid = userid_item;
                  break;
                default:
                  TProtocolUtil.skip(_iprot, _field.type);
              }
              builder.userid(userid);
              break;
            default:
              TProtocolUtil.skip(_iprot, _field.type);
          }
          _iprot.readFieldEnd();
        }
      }
      _iprot.readStructEnd();
      try {
        return builder.build();
      } catch (IllegalStateException stateEx) {
        throw new TProtocolException(stateEx.getMessage());
      }
    }

    public void encode(ClickParam struct, TProtocol oprot) throws org.apache.thrift.TException {
      struct.write(oprot);
    }
  };

  public static ClickParam decode(TProtocol _iprot) throws org.apache.thrift.TException {
    return CODEC.decode(_iprot);
  }

  public static void encode(ClickParam struct, TProtocol oprot) throws org.apache.thrift.TException {
    CODEC.encode(struct, oprot);
  }

  public ClickParam(
  int itemId,
  String itemName,
  Option<RecommendType> type,
  String userid
  ) {
    this.itemId = itemId;
    this.itemName = itemName;
    this.type = type;
    this.userid = userid;
  }

  public int getItemId() {
    return this.itemId;
  }
  public String getItemName() {
    return this.itemName;
  }
  public RecommendType getType() {
    return this.type.get();
  }
  public String getUserid() {
    return this.userid;
  }

  public void write(TProtocol _oprot) throws org.apache.thrift.TException {
    validate();
    _oprot.writeStructBegin(STRUCT);
      _oprot.writeFieldBegin(ItemIdField);
      Integer itemId_item = itemId;
      _oprot.writeI32(itemId_item);
      _oprot.writeFieldEnd();
      _oprot.writeFieldBegin(ItemNameField);
      String itemName_item = itemName;
      _oprot.writeString(itemName_item);
      _oprot.writeFieldEnd();
    if (type.isDefined()) {  _oprot.writeFieldBegin(TypeField);
      RecommendType type_item = type.get();
      _oprot.writeI32(type_item.getValue());
      _oprot.writeFieldEnd();
    }
      _oprot.writeFieldBegin(UseridField);
      String userid_item = userid;
      _oprot.writeString(userid_item);
      _oprot.writeFieldEnd();
    _oprot.writeFieldStop();
    _oprot.writeStructEnd();
  }

  private void validate() throws org.apache.thrift.protocol.TProtocolException {
  if (this.itemName == null)
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'itemName' cannot be null");
  }

  public boolean equals(Object other) {
    if (!(other instanceof ClickParam)) return false;
    ClickParam that = (ClickParam) other;
    return
      this.itemId == that.itemId &&

this.itemName.equals(that.itemName) &&

this.type.equals(that.type) &&

this.userid.equals(that.userid)
;
  }

  public String toString() {
    return "ClickParam(" + this.itemId + "," + this.itemName + "," + this.type + "," + this.userid + ")";
  }

  public int hashCode() {
    int hash = 1;
    hash = hash * new Integer(this.itemId).hashCode();
    hash = hash * (this.itemName == null ? 0 : this.itemName.hashCode());
    hash = hash * (this.type.isDefined() ? 0 : this.type.get().hashCode());
    hash = hash * (this.userid == null ? 0 : this.userid.hashCode());
    return hash;
  }
}