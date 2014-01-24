package com.ctrip.gs.recommendation.thrift;

import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;

import com.twitter.scrooge.ThriftStruct;
import com.twitter.scrooge.ThriftStructCodec3;
import com.twitter.scrooge.Utilities;

public class RecommendItemList implements ThriftStruct {
  private static final TStruct STRUCT = new TStruct("RecommendItemList");
  private static final TField TypeParamField = new TField("typeParam", TType.STRUCT, (short) 1);
  final RecommendTypeParam typeParam;
  private static final TField ItemsField = new TField("items", TType.LIST, (short) 2);
  final List<RecommendItem> items;

  public static class Builder {
    private RecommendTypeParam _typeParam = null;
    private Boolean _got_typeParam = false;

    public Builder typeParam(RecommendTypeParam value) {
      this._typeParam = value;
      this._got_typeParam = true;
      return this;
    }
    private List<RecommendItem> _items = Utilities.makeList();
    private Boolean _got_items = false;

    public Builder items(List<RecommendItem> value) {
      this._items = value;
      this._got_items = true;
      return this;
    }

    public RecommendItemList build() {
      if (!_got_typeParam)
      throw new IllegalStateException("Required field 'typeParam' was not found for struct RecommendItemList");
      if (!_got_items)
      throw new IllegalStateException("Required field 'items' was not found for struct RecommendItemList");
      return new RecommendItemList(
        this._typeParam,
        this._items    );
    }
  }

  public Builder copy() {
    Builder builder = new Builder();
    builder.typeParam(this.typeParam);
    builder.items(this.items);
    return builder;
  }

  public static ThriftStructCodec3<RecommendItemList> CODEC = new ThriftStructCodec3<RecommendItemList>() {
    public RecommendItemList decode(TProtocol _iprot) throws org.apache.thrift.TException {
      Builder builder = new Builder();
      RecommendTypeParam typeParam = null;
      List<RecommendItem> items = Utilities.makeList();
      Boolean _done = false;
      _iprot.readStructBegin();
      while (!_done) {
        TField _field = _iprot.readFieldBegin();
        if (_field.type == TType.STOP) {
          _done = true;
        } else {
          switch (_field.id) {
            case 1: /* typeParam */
              switch (_field.type) {
                case TType.STRUCT:
                  RecommendTypeParam typeParam_item;
                  typeParam_item = RecommendTypeParam.decode(_iprot);
                  typeParam = typeParam_item;
                  break;
                default:
                  TProtocolUtil.skip(_iprot, _field.type);
              }
              builder.typeParam(typeParam);
              break;
            case 2: /* items */
              switch (_field.type) {
                case TType.LIST:
                  List<RecommendItem> items_item;
                  TList _list_items_item = _iprot.readListBegin();
                  items_item = new ArrayList<RecommendItem>();
                  int _i_items_item = 0;
                  RecommendItem _items_item_element;
                  while (_i_items_item < _list_items_item.size) {
                    _items_item_element = RecommendItem.decode(_iprot);
                    items_item.add(_items_item_element);
                    _i_items_item += 1;
                  }
                  _iprot.readListEnd();
                  items = items_item;
                  break;
                default:
                  TProtocolUtil.skip(_iprot, _field.type);
              }
              builder.items(items);
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

    public void encode(RecommendItemList struct, TProtocol oprot) throws org.apache.thrift.TException {
      struct.write(oprot);
    }
  };

  public static RecommendItemList decode(TProtocol _iprot) throws org.apache.thrift.TException {
    return CODEC.decode(_iprot);
  }

  public static void encode(RecommendItemList struct, TProtocol oprot) throws org.apache.thrift.TException {
    CODEC.encode(struct, oprot);
  }

  public RecommendItemList(
  RecommendTypeParam typeParam,
  List<RecommendItem> items
  ) {
    this.typeParam = typeParam;
    this.items = items;
  }

  public RecommendTypeParam getTypeParam() {
    return this.typeParam;
  }
  public List<RecommendItem> getItems() {
    return this.items;
  }

  public void write(TProtocol _oprot) throws org.apache.thrift.TException {
    validate();
    _oprot.writeStructBegin(STRUCT);
      _oprot.writeFieldBegin(TypeParamField);
      RecommendTypeParam typeParam_item = typeParam;
      typeParam_item.write(_oprot);
      _oprot.writeFieldEnd();
      _oprot.writeFieldBegin(ItemsField);
      List<RecommendItem> items_item = items;
      _oprot.writeListBegin(new TList(TType.STRUCT, items_item.size()));
      for (RecommendItem _items_item_element : items_item) {
        _items_item_element.write(_oprot);
      }
      _oprot.writeListEnd();
      _oprot.writeFieldEnd();
    _oprot.writeFieldStop();
    _oprot.writeStructEnd();
  }

  private void validate() throws org.apache.thrift.protocol.TProtocolException {
  if (this.typeParam == null)
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'typeParam' cannot be null");
  if (this.items == null)
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'items' cannot be null");
  }

  public boolean equals(Object other) {
    if (!(other instanceof RecommendItemList)) return false;
    RecommendItemList that = (RecommendItemList) other;
    return
this.typeParam.equals(that.typeParam) &&
this.items.equals(that.items);
  }

  public String toString() {
    return "RecommendItemList(" + this.typeParam + "," + this.items + ")";
  }

  public int hashCode() {
    int hash = 1;
    hash = hash * (this.typeParam == null ? 0 : this.typeParam.hashCode());
    hash = hash * (this.items == null ? 0 : this.items.hashCode());
    return hash;
  }
}