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

import com.twitter.scrooge.Option;
import com.twitter.scrooge.ThriftStruct;
import com.twitter.scrooge.ThriftStructCodec3;
import com.twitter.scrooge.Utilities;

public class RecommendParam implements ThriftStruct {
  private static final TStruct STRUCT = new TStruct("RecommendParam");
  private static final TField UrlField = new TField("url", TType.STRING, (short) 1);
  final String url;
  private static final TField TitleField = new TField("title", TType.STRING, (short) 2);
  final Option<String> title;
  private static final TField KeywordsField = new TField("keywords", TType.STRING, (short) 3);
  final Option<String> keywords;
  private static final TField TypesField = new TField("types", TType.LIST, (short) 4);
  final List<RecommendTypeParam> types;

  public static class Builder {
    private String _url = null;
    private Boolean _got_url = false;

    public Builder url(String value) {
      this._url = value;
      this._got_url = true;
      return this;
    }
    private String _title = null;
    private Boolean _got_title = false;

    public Builder title(String value) {
      this._title = value;
      this._got_title = true;
      return this;
    }
    private String _keywords = null;
    private Boolean _got_keywords = false;

    public Builder keywords(String value) {
      this._keywords = value;
      this._got_keywords = true;
      return this;
    }
    private List<RecommendTypeParam> _types = Utilities.makeList();
    private Boolean _got_types = false;

    public Builder types(List<RecommendTypeParam> value) {
      this._types = value;
      this._got_types = true;
      return this;
    }

    public RecommendParam build() {
      if (!_got_url)
      throw new IllegalStateException("Required field 'url' was not found for struct RecommendParam");
      if (!_got_types)
      throw new IllegalStateException("Required field 'types' was not found for struct RecommendParam");
      return new RecommendParam(
        this._url,
      Option.make(this._got_title, this._title),
      Option.make(this._got_keywords, this._keywords),
        this._types    );
    }
  }

  public Builder copy() {
    Builder builder = new Builder();
    builder.url(this.url);
    if (this.title.isDefined()) builder.title(this.title.get());
    if (this.keywords.isDefined()) builder.keywords(this.keywords.get());
    builder.types(this.types);
    return builder;
  }

  public static ThriftStructCodec3<RecommendParam> CODEC = new ThriftStructCodec3<RecommendParam>() {
    public RecommendParam decode(TProtocol _iprot) throws org.apache.thrift.TException {
      Builder builder = new Builder();
      String url = null;
      String title = null;
      String keywords = null;
      List<RecommendTypeParam> types = Utilities.makeList();
      Boolean _done = false;
      _iprot.readStructBegin();
      while (!_done) {
        TField _field = _iprot.readFieldBegin();
        if (_field.type == TType.STOP) {
          _done = true;
        } else {
          switch (_field.id) {
            case 1: /* url */
              switch (_field.type) {
                case TType.STRING:
                  String url_item;
                  url_item = _iprot.readString();
                  url = url_item;
                  break;
                default:
                  TProtocolUtil.skip(_iprot, _field.type);
              }
              builder.url(url);
              break;
            case 2: /* title */
              switch (_field.type) {
                case TType.STRING:
                  String title_item;
                  title_item = _iprot.readString();
                  title = title_item;
                  break;
                default:
                  TProtocolUtil.skip(_iprot, _field.type);
              }
              builder.title(title);
              break;
            case 3: /* keywords */
              switch (_field.type) {
                case TType.STRING:
                  String keywords_item;
                  keywords_item = _iprot.readString();
                  keywords = keywords_item;
                  break;
                default:
                  TProtocolUtil.skip(_iprot, _field.type);
              }
              builder.keywords(keywords);
              break;
            case 4: /* types */
              switch (_field.type) {
                case TType.LIST:
                  List<RecommendTypeParam> types_item;
                  TList _list_types_item = _iprot.readListBegin();
                  types_item = new ArrayList<RecommendTypeParam>();
                  int _i_types_item = 0;
                  RecommendTypeParam _types_item_element;
                  while (_i_types_item < _list_types_item.size) {
                    _types_item_element = RecommendTypeParam.decode(_iprot);
                    types_item.add(_types_item_element);
                    _i_types_item += 1;
                  }
                  _iprot.readListEnd();
                  types = types_item;
                  break;
                default:
                  TProtocolUtil.skip(_iprot, _field.type);
              }
              builder.types(types);
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

    public void encode(RecommendParam struct, TProtocol oprot) throws org.apache.thrift.TException {
      struct.write(oprot);
    }
  };

  public static RecommendParam decode(TProtocol _iprot) throws org.apache.thrift.TException {
    return CODEC.decode(_iprot);
  }

  public static void encode(RecommendParam struct, TProtocol oprot) throws org.apache.thrift.TException {
    CODEC.encode(struct, oprot);
  }

  public RecommendParam(
  String url,
  Option<String> title,
  Option<String> keywords,
  List<RecommendTypeParam> types
  ) {
    this.url = url;
    this.title = title;
    this.keywords = keywords;
    this.types = types;
  }

  public String getUrl() {
    return this.url;
  }
  public String getTitle() {
    return this.title.get();
  }
  public String getKeywords() {
    return this.keywords.get();
  }
  public List<RecommendTypeParam> getTypes() {
    return this.types;
  }

  public void write(TProtocol _oprot) throws org.apache.thrift.TException {
    validate();
    _oprot.writeStructBegin(STRUCT);
      _oprot.writeFieldBegin(UrlField);
      String url_item = url;
      _oprot.writeString(url_item);
      _oprot.writeFieldEnd();
    if (title.isDefined()) {  _oprot.writeFieldBegin(TitleField);
      String title_item = title.get();
      _oprot.writeString(title_item);
      _oprot.writeFieldEnd();
    }
    if (keywords.isDefined()) {  _oprot.writeFieldBegin(KeywordsField);
      String keywords_item = keywords.get();
      _oprot.writeString(keywords_item);
      _oprot.writeFieldEnd();
    }
      _oprot.writeFieldBegin(TypesField);
      List<RecommendTypeParam> types_item = types;
      _oprot.writeListBegin(new TList(TType.STRUCT, types_item.size()));
      for (RecommendTypeParam _types_item_element : types_item) {
        _types_item_element.write(_oprot);
      }
      _oprot.writeListEnd();
      _oprot.writeFieldEnd();
    _oprot.writeFieldStop();
    _oprot.writeStructEnd();
  }

  private void validate() throws org.apache.thrift.protocol.TProtocolException {
  if (this.url == null)
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'url' cannot be null");
  if (this.types == null)
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'types' cannot be null");
  }

  public boolean equals(Object other) {
    if (!(other instanceof RecommendParam)) return false;
    RecommendParam that = (RecommendParam) other;
    return
this.url.equals(that.url) &&
this.title.equals(that.title) &&
this.keywords.equals(that.keywords) &&
this.types.equals(that.types);
  }

  public String toString() {
    return "RecommendParam(" + this.url + "," + this.title + "," + this.keywords + "," + this.types + ")";
  }

  public int hashCode() {
    int hash = 1;
    hash = hash * (this.url == null ? 0 : this.url.hashCode());
    hash = hash * (this.title.isDefined() ? 0 : this.title.get().hashCode());
    hash = hash * (this.keywords.isDefined() ? 0 : this.keywords.get().hashCode());
    hash = hash * (this.types == null ? 0 : this.types.hashCode());
    return hash;
  }
}