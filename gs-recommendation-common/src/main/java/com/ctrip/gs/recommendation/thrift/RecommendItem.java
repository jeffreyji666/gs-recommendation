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

public class RecommendItem implements ThriftStruct {
  private static final TStruct STRUCT = new TStruct("RecommendItem");
  private static final TField ItemIdField = new TField("itemId", TType.I32, (short) 1);
  final int itemId;
  private static final TField ItemNameField = new TField("itemName", TType.STRING, (short) 2);
  final String itemName;
  private static final TField PicField = new TField("pic", TType.STRING, (short) 3);
  final Option<String> pic;
  private static final TField ScoreField = new TField("score", TType.DOUBLE, (short) 4);
  final Option<Double> score;

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
    private String _pic = null;
    private Boolean _got_pic = false;

    public Builder pic(String value) {
      this._pic = value;
      this._got_pic = true;
      return this;
    }
    private double _score = 0.0;
    private Boolean _got_score = false;

    public Builder score(double value) {
      this._score = value;
      this._got_score = true;
      return this;
    }

    public RecommendItem build() {
      if (!_got_itemId)
      throw new IllegalStateException("Required field 'itemId' was not found for struct RecommendItem");
      if (!_got_itemName)
      throw new IllegalStateException("Required field 'itemName' was not found for struct RecommendItem");
      return new RecommendItem(
        this._itemId,
        this._itemName,
      Option.make(this._got_pic, this._pic),
      Option.make(this._got_score, this._score)    );
    }
  }

  public Builder copy() {
    Builder builder = new Builder();
    builder.itemId(this.itemId);
    builder.itemName(this.itemName);
    if (this.pic.isDefined()) builder.pic(this.pic.get());
    if (this.score.isDefined()) builder.score(this.score.get());
    return builder;
  }

  public static ThriftStructCodec3<RecommendItem> CODEC = new ThriftStructCodec3<RecommendItem>() {
    public RecommendItem decode(TProtocol _iprot) throws org.apache.thrift.TException {
      Builder builder = new Builder();
      int itemId = 0;
      String itemName = null;
      String pic = null;
      double score = 0.0;
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
            case 3: /* pic */
              switch (_field.type) {
                case TType.STRING:
                  String pic_item;
                  pic_item = _iprot.readString();
                  pic = pic_item;
                  break;
                default:
                  TProtocolUtil.skip(_iprot, _field.type);
              }
              builder.pic(pic);
              break;
            case 4: /* score */
              switch (_field.type) {
                case TType.DOUBLE:
                  Double score_item;
                  score_item = _iprot.readDouble();
                  score = score_item;
                  break;
                default:
                  TProtocolUtil.skip(_iprot, _field.type);
              }
              builder.score(score);
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

    public void encode(RecommendItem struct, TProtocol oprot) throws org.apache.thrift.TException {
      struct.write(oprot);
    }
  };

  public static RecommendItem decode(TProtocol _iprot) throws org.apache.thrift.TException {
    return CODEC.decode(_iprot);
  }

  public static void encode(RecommendItem struct, TProtocol oprot) throws org.apache.thrift.TException {
    CODEC.encode(struct, oprot);
  }

  public RecommendItem(
  int itemId,
  String itemName,
  Option<String> pic,
  Option<Double> score
  ) {
    this.itemId = itemId;
    this.itemName = itemName;
    this.pic = pic;
    this.score = score;
  }

  public int getItemId() {
    return this.itemId;
  }
  public String getItemName() {
    return this.itemName;
  }
  public String getPic() {
    return this.pic.get();
  }
  public double getScore() {
    return this.score.get();
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
    if (pic.isDefined()) {  _oprot.writeFieldBegin(PicField);
      String pic_item = pic.get();
      _oprot.writeString(pic_item);
      _oprot.writeFieldEnd();
    }
    if (score.isDefined()) {  _oprot.writeFieldBegin(ScoreField);
      Double score_item = score.get();
      _oprot.writeDouble(score_item);
      _oprot.writeFieldEnd();
    }
    _oprot.writeFieldStop();
    _oprot.writeStructEnd();
  }

  private void validate() throws org.apache.thrift.protocol.TProtocolException {
  if (this.itemName == null)
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'itemName' cannot be null");
  }

  public boolean equals(Object other) {
    if (!(other instanceof RecommendItem)) return false;
    RecommendItem that = (RecommendItem) other;
    return
      this.itemId == that.itemId &&

this.itemName.equals(that.itemName) &&

this.pic.equals(that.pic) &&

      this.score.equals(that.score)
;
  }

  public String toString() {
    return "RecommendItem(" + this.itemId + "," + this.itemName + "," + this.pic + "," + this.score + ")";
  }

  public int hashCode() {
    int hash = 1;
    hash = hash * new Integer(this.itemId).hashCode();
    hash = hash * (this.itemName == null ? 0 : this.itemName.hashCode());
    hash = hash * (this.pic.isDefined() ? 0 : this.pic.get().hashCode());
    hash = hash * (this.score.isDefined() ? 0 : new Double(this.score.get()).hashCode());
    return hash;
  }
}