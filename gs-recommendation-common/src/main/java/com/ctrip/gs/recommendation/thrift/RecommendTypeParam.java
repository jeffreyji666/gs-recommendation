package com.ctrip.gs.recommendation.thrift;

import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;

import com.twitter.scrooge.ThriftStruct;
import com.twitter.scrooge.ThriftStructCodec3;

public class RecommendTypeParam implements ThriftStruct {
  private static final TStruct STRUCT = new TStruct("RecommendTypeParam");
  private static final TField OrderField = new TField("order", TType.I32, (short) 1);
  final int order;
  private static final TField RecommendTypeField = new TField("recommendType", TType.I32, (short) 2);
  final RecommendType recommendType;
  private static final TField CountField = new TField("count", TType.I32, (short) 3);
  final int count;

  public static class Builder {
    private int _order = 0;
    private Boolean _got_order = false;

    public Builder order(int value) {
      this._order = value;
      this._got_order = true;
      return this;
    }
    private RecommendType _recommendType = null;
    private Boolean _got_recommendType = false;

    public Builder recommendType(RecommendType value) {
      this._recommendType = value;
      this._got_recommendType = true;
      return this;
    }
    private int _count = 5;
    private Boolean _got_count = false;

    public Builder count(int value) {
      this._count = value;
      this._got_count = true;
      return this;
    }

    public RecommendTypeParam build() {
      if (!_got_order)
      throw new IllegalStateException("Required field 'order' was not found for struct RecommendTypeParam");
      if (!_got_recommendType)
      throw new IllegalStateException("Required field 'recommendType' was not found for struct RecommendTypeParam");
      if (!_got_count)
      throw new IllegalStateException("Required field 'count' was not found for struct RecommendTypeParam");
      return new RecommendTypeParam(
        this._order,
        this._recommendType,
        this._count    );
    }
  }

  public Builder copy() {
    Builder builder = new Builder();
    builder.order(this.order);
    builder.recommendType(this.recommendType);
    builder.count(this.count);
    return builder;
  }

  public static ThriftStructCodec3<RecommendTypeParam> CODEC = new ThriftStructCodec3<RecommendTypeParam>() {
    public RecommendTypeParam decode(TProtocol _iprot) throws org.apache.thrift.TException {
      Builder builder = new Builder();
      int order = 0;
      RecommendType recommendType = null;
      int count = 5;
      Boolean _done = false;
      _iprot.readStructBegin();
      while (!_done) {
        TField _field = _iprot.readFieldBegin();
        if (_field.type == TType.STOP) {
          _done = true;
        } else {
          switch (_field.id) {
            case 1: /* order */
              switch (_field.type) {
                case TType.I32:
                  Integer order_item;
                  order_item = _iprot.readI32();
                  order = order_item;
                  break;
                default:
                  TProtocolUtil.skip(_iprot, _field.type);
              }
              builder.order(order);
              break;
            case 2: /* recommendType */
              switch (_field.type) {
                case TType.I32:
                  RecommendType recommendType_item;
                  recommendType_item = RecommendType.findByValue(_iprot.readI32());
                  recommendType = recommendType_item;
                  break;
                default:
                  TProtocolUtil.skip(_iprot, _field.type);
              }
              builder.recommendType(recommendType);
              break;
            case 3: /* count */
              switch (_field.type) {
                case TType.I32:
                  Integer count_item;
                  count_item = _iprot.readI32();
                  count = count_item;
                  break;
                default:
                  TProtocolUtil.skip(_iprot, _field.type);
              }
              builder.count(count);
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

    public void encode(RecommendTypeParam struct, TProtocol oprot) throws org.apache.thrift.TException {
      struct.write(oprot);
    }
  };

  public static RecommendTypeParam decode(TProtocol _iprot) throws org.apache.thrift.TException {
    return CODEC.decode(_iprot);
  }

  public static void encode(RecommendTypeParam struct, TProtocol oprot) throws org.apache.thrift.TException {
    CODEC.encode(struct, oprot);
  }

  public RecommendTypeParam(
  int order,
  RecommendType recommendType,
  int count
  ) {
    this.order = order;
    this.recommendType = recommendType;
    this.count = count;
  }

  public int getOrder() {
    return this.order;
  }
  public RecommendType getRecommendType() {
    return this.recommendType;
  }
  public int getCount() {
    return this.count;
  }

  public void write(TProtocol _oprot) throws org.apache.thrift.TException {
    validate();
    _oprot.writeStructBegin(STRUCT);
      _oprot.writeFieldBegin(OrderField);
      Integer order_item = order;
      _oprot.writeI32(order_item);
      _oprot.writeFieldEnd();
      _oprot.writeFieldBegin(RecommendTypeField);
      RecommendType recommendType_item = recommendType;
      _oprot.writeI32(recommendType_item.getValue());
      _oprot.writeFieldEnd();
      _oprot.writeFieldBegin(CountField);
      Integer count_item = count;
      _oprot.writeI32(count_item);
      _oprot.writeFieldEnd();
    _oprot.writeFieldStop();
    _oprot.writeStructEnd();
  }

  private void validate() throws org.apache.thrift.protocol.TProtocolException {
  if (this.recommendType == null)
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'recommendType' cannot be null");
  }

  public boolean equals(Object other) {
    if (!(other instanceof RecommendTypeParam)) return false;
    RecommendTypeParam that = (RecommendTypeParam) other;
    return
      this.order == that.order &&

this.recommendType.equals(that.recommendType) &&

      this.count == that.count
;
  }

  public String toString() {
    return "RecommendTypeParam(" + this.order + "," + this.recommendType + "," + this.count + ")";
  }

  public int hashCode() {
    int hash = 1;
    hash = hash * new Integer(this.order).hashCode();
    hash = hash * (this.recommendType == null ? 0 : this.recommendType.hashCode());
    hash = hash * new Integer(this.count).hashCode();
    return hash;
  }
}