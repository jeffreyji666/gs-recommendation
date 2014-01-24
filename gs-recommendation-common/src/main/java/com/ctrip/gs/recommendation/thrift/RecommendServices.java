package com.ctrip.gs.recommendation.thrift;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.apache.thrift.transport.TTransport;

import com.twitter.finagle.Service;
import com.twitter.finagle.SourcedException;
import com.twitter.finagle.stats.Counter;
import com.twitter.finagle.stats.NullStatsReceiver;
import com.twitter.finagle.stats.StatsReceiver;
import com.twitter.finagle.thrift.ThriftClientRequest;
import com.twitter.scrooge.Option;
import com.twitter.scrooge.ThriftStruct;
import com.twitter.scrooge.ThriftStructCodec3;
import com.twitter.scrooge.Utilities;
import com.twitter.util.Function;
import com.twitter.util.Function2;
import com.twitter.util.Future;
import com.twitter.util.FutureEventListener;

public class RecommendServices {
  public interface Iface {
    public List<RecommendItemList> recommend(RecommendParam param);
    public Void click(ClickParam param);
  }

  public interface FutureIface {
    public Future<List<RecommendItemList>> recommend(RecommendParam param);
    public Future<Void> click(ClickParam param);
  }

  static class recommend_args implements ThriftStruct {
    private static final TStruct STRUCT = new TStruct("recommend_args");
    private static final TField ParamField = new TField("param", TType.STRUCT, (short) 1);
    final RecommendParam param;
  
    public static class Builder {
      private RecommendParam _param = null;
      private Boolean _got_param = false;
  
      public Builder param(RecommendParam value) {
        this._param = value;
        this._got_param = true;
        return this;
      }
  
      public recommend_args build() {
        return new recommend_args(
          this._param    );
      }
    }
  
    public Builder copy() {
      Builder builder = new Builder();
      builder.param(this.param);
      return builder;
    }
  
    public static ThriftStructCodec3<recommend_args> CODEC = new ThriftStructCodec3<recommend_args>() {
      public recommend_args decode(TProtocol _iprot) throws org.apache.thrift.TException {
        Builder builder = new Builder();
        RecommendParam param = null;
        Boolean _done = false;
        _iprot.readStructBegin();
        while (!_done) {
          TField _field = _iprot.readFieldBegin();
          if (_field.type == TType.STOP) {
            _done = true;
          } else {
            switch (_field.id) {
              case 1: /* param */
                switch (_field.type) {
                  case TType.STRUCT:
                    RecommendParam param_item;
                    param_item = RecommendParam.decode(_iprot);
                    param = param_item;
                    break;
                  default:
                    TProtocolUtil.skip(_iprot, _field.type);
                }
                builder.param(param);
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
  
      public void encode(recommend_args struct, TProtocol oprot) throws org.apache.thrift.TException {
        struct.write(oprot);
      }
    };
  
    public static recommend_args decode(TProtocol _iprot) throws org.apache.thrift.TException {
      return CODEC.decode(_iprot);
    }
  
    public static void encode(recommend_args struct, TProtocol oprot) throws org.apache.thrift.TException {
      CODEC.encode(struct, oprot);
    }
  
    public recommend_args(
    RecommendParam param
    ) {
      this.param = param;
    }
  
    public RecommendParam getParam() {
      return this.param;
    }
  
    public void write(TProtocol _oprot) throws org.apache.thrift.TException {
      validate();
      _oprot.writeStructBegin(STRUCT);
        _oprot.writeFieldBegin(ParamField);
        RecommendParam param_item = param;
        param_item.write(_oprot);
        _oprot.writeFieldEnd();
      _oprot.writeFieldStop();
      _oprot.writeStructEnd();
    }
  
    private void validate() throws org.apache.thrift.protocol.TProtocolException {
    }
  
    public boolean equals(Object other) {
      if (!(other instanceof recommend_args)) return false;
      recommend_args that = (recommend_args) other;
      return
  this.param.equals(that.param);
    }
  
    public String toString() {
      return "recommend_args(" + this.param + ")";
    }
  
    public int hashCode() {
      int hash = 1;
      hash = hash * (this.param == null ? 0 : this.param.hashCode());
      return hash;
    }
  }
  static class recommend_result implements ThriftStruct {
    private static final TStruct STRUCT = new TStruct("recommend_result");
    private static final TField SuccessField = new TField("success", TType.LIST, (short) 0);
    final Option<List<RecommendItemList>> success;
  
    public static class Builder {
      private List<RecommendItemList> _success = Utilities.makeList();
      private Boolean _got_success = false;
  
      public Builder success(List<RecommendItemList> value) {
        this._success = value;
        this._got_success = true;
        return this;
      }
  
      public recommend_result build() {
        return new recommend_result(
        Option.make(this._got_success, this._success)    );
      }
    }
  
    public Builder copy() {
      Builder builder = new Builder();
      if (this.success.isDefined()) builder.success(this.success.get());
      return builder;
    }
  
    public static ThriftStructCodec3<recommend_result> CODEC = new ThriftStructCodec3<recommend_result>() {
      public recommend_result decode(TProtocol _iprot) throws org.apache.thrift.TException {
        Builder builder = new Builder();
        List<RecommendItemList> success = Utilities.makeList();
        Boolean _done = false;
        _iprot.readStructBegin();
        while (!_done) {
          TField _field = _iprot.readFieldBegin();
          if (_field.type == TType.STOP) {
            _done = true;
          } else {
            switch (_field.id) {
              case 0: /* success */
                switch (_field.type) {
                  case TType.LIST:
                    List<RecommendItemList> success_item;
                    TList _list_success_item = _iprot.readListBegin();
                    success_item = new ArrayList<RecommendItemList>();
                    int _i_success_item = 0;
                    RecommendItemList _success_item_element;
                    while (_i_success_item < _list_success_item.size) {
                      _success_item_element = RecommendItemList.decode(_iprot);
                      success_item.add(_success_item_element);
                      _i_success_item += 1;
                    }
                    _iprot.readListEnd();
                    success = success_item;
                    break;
                  default:
                    TProtocolUtil.skip(_iprot, _field.type);
                }
                builder.success(success);
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
  
      public void encode(recommend_result struct, TProtocol oprot) throws org.apache.thrift.TException {
        struct.write(oprot);
      }
    };
  
    public static recommend_result decode(TProtocol _iprot) throws org.apache.thrift.TException {
      return CODEC.decode(_iprot);
    }
  
    public static void encode(recommend_result struct, TProtocol oprot) throws org.apache.thrift.TException {
      CODEC.encode(struct, oprot);
    }
  
    public recommend_result(
    Option<List<RecommendItemList>> success
    ) {
      this.success = success;
    }
  
    public List<RecommendItemList> getSuccess() {
      return this.success.get();
    }
  
    public void write(TProtocol _oprot) throws org.apache.thrift.TException {
      validate();
      _oprot.writeStructBegin(STRUCT);
      if (success.isDefined()) {  _oprot.writeFieldBegin(SuccessField);
        List<RecommendItemList> success_item = success.get();
        _oprot.writeListBegin(new TList(TType.STRUCT, success_item.size()));
        for (RecommendItemList _success_item_element : success_item) {
          _success_item_element.write(_oprot);
        }
        _oprot.writeListEnd();
        _oprot.writeFieldEnd();
      }
      _oprot.writeFieldStop();
      _oprot.writeStructEnd();
    }
  
    private void validate() throws org.apache.thrift.protocol.TProtocolException {
    }
  
    public boolean equals(Object other) {
      if (!(other instanceof recommend_result)) return false;
      recommend_result that = (recommend_result) other;
      return
  this.success.equals(that.success);
    }
  
    public String toString() {
      return "recommend_result(" + this.success + ")";
    }
  
    public int hashCode() {
      int hash = 1;
      hash = hash * (this.success.isDefined() ? 0 : this.success.get().hashCode());
      return hash;
    }
  }
  static class click_args implements ThriftStruct {
    private static final TStruct STRUCT = new TStruct("click_args");
    private static final TField ParamField = new TField("param", TType.STRUCT, (short) 1);
    final ClickParam param;
  
    public static class Builder {
      private ClickParam _param = null;
      private Boolean _got_param = false;
  
      public Builder param(ClickParam value) {
        this._param = value;
        this._got_param = true;
        return this;
      }
  
      public click_args build() {
        return new click_args(
          this._param    );
      }
    }
  
    public Builder copy() {
      Builder builder = new Builder();
      builder.param(this.param);
      return builder;
    }
  
    public static ThriftStructCodec3<click_args> CODEC = new ThriftStructCodec3<click_args>() {
      public click_args decode(TProtocol _iprot) throws org.apache.thrift.TException {
        Builder builder = new Builder();
        ClickParam param = null;
        Boolean _done = false;
        _iprot.readStructBegin();
        while (!_done) {
          TField _field = _iprot.readFieldBegin();
          if (_field.type == TType.STOP) {
            _done = true;
          } else {
            switch (_field.id) {
              case 1: /* param */
                switch (_field.type) {
                  case TType.STRUCT:
                    ClickParam param_item;
                    param_item = ClickParam.decode(_iprot);
                    param = param_item;
                    break;
                  default:
                    TProtocolUtil.skip(_iprot, _field.type);
                }
                builder.param(param);
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
  
      public void encode(click_args struct, TProtocol oprot) throws org.apache.thrift.TException {
        struct.write(oprot);
      }
    };
  
    public static click_args decode(TProtocol _iprot) throws org.apache.thrift.TException {
      return CODEC.decode(_iprot);
    }
  
    public static void encode(click_args struct, TProtocol oprot) throws org.apache.thrift.TException {
      CODEC.encode(struct, oprot);
    }
  
    public click_args(
    ClickParam param
    ) {
      this.param = param;
    }
  
    public ClickParam getParam() {
      return this.param;
    }
  
    public void write(TProtocol _oprot) throws org.apache.thrift.TException {
      validate();
      _oprot.writeStructBegin(STRUCT);
        _oprot.writeFieldBegin(ParamField);
        ClickParam param_item = param;
        param_item.write(_oprot);
        _oprot.writeFieldEnd();
      _oprot.writeFieldStop();
      _oprot.writeStructEnd();
    }
  
    private void validate() throws org.apache.thrift.protocol.TProtocolException {
    }
  
    public boolean equals(Object other) {
      if (!(other instanceof click_args)) return false;
      click_args that = (click_args) other;
      return
  this.param.equals(that.param);
    }
  
    public String toString() {
      return "click_args(" + this.param + ")";
    }
  
    public int hashCode() {
      int hash = 1;
      hash = hash * (this.param == null ? 0 : this.param.hashCode());
      return hash;
    }
  }
  static class click_result implements ThriftStruct {
    private static final TStruct STRUCT = new TStruct("click_result");
  
    public static class Builder {
  
      public click_result build() {
        return new click_result(
      );
      }
    }
  
    public Builder copy() {
      Builder builder = new Builder();
      return builder;
    }
  
    public static ThriftStructCodec3<click_result> CODEC = new ThriftStructCodec3<click_result>() {
      public click_result decode(TProtocol _iprot) throws org.apache.thrift.TException {
        Builder builder = new Builder();
        Boolean _done = false;
        _iprot.readStructBegin();
        while (!_done) {
          TField _field = _iprot.readFieldBegin();
          if (_field.type == TType.STOP) {
            _done = true;
          } else {
            switch (_field.id) {
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
  
      public void encode(click_result struct, TProtocol oprot) throws org.apache.thrift.TException {
        struct.write(oprot);
      }
    };
  
    public static click_result decode(TProtocol _iprot) throws org.apache.thrift.TException {
      return CODEC.decode(_iprot);
    }
  
    public static void encode(click_result struct, TProtocol oprot) throws org.apache.thrift.TException {
      CODEC.encode(struct, oprot);
    }
  
    public click_result(
    ) {
    }
  
  
    public void write(TProtocol _oprot) throws org.apache.thrift.TException {
      validate();
      _oprot.writeStructBegin(STRUCT);
      _oprot.writeFieldStop();
      _oprot.writeStructEnd();
    }
  
    private void validate() throws org.apache.thrift.protocol.TProtocolException {
    }
  
    public boolean equals(Object other) {
      return this == other;
    }
  
    public String toString() {
      return "click_result()";
    }
  
    public int hashCode() {
      return super.hashCode();
    }
  }
  public static class FinagledClient implements FutureIface {
    private com.twitter.finagle.Service<ThriftClientRequest, byte[]> service;
    private String serviceName;
    private TProtocolFactory protocolFactory /* new TBinaryProtocol.Factory */;
    private StatsReceiver scopedStats = new NullStatsReceiver();
  
    public FinagledClient(
      com.twitter.finagle.Service<ThriftClientRequest, byte[]> service,
      TProtocolFactory protocolFactory /* new TBinaryProtocol.Factory */,
      String serviceName) {
      this.service = service;
      this.serviceName = serviceName;
      this.protocolFactory = protocolFactory;
    }
  
    // ----- boilerplate that should eventually be moved into finagle:
  
    protected ThriftClientRequest encodeRequest(String name, ThriftStruct args) {
      TMemoryBuffer buf = new TMemoryBuffer(512);
      TProtocol oprot = protocolFactory.getProtocol(buf);
  
      try {
        oprot.writeMessageBegin(new TMessage(name, TMessageType.CALL, 0));
        args.write(oprot);
        oprot.writeMessageEnd();
      } catch (TException e) {
        // not real.
      }
  
      byte[] bytes = Arrays.copyOfRange(buf.getArray(), 0, buf.length());
      return new ThriftClientRequest(bytes, false);
    }
  
    protected <T extends ThriftStruct> T decodeResponse(byte[] resBytes, ThriftStructCodec3<T> codec) throws TException {
      TProtocol iprot = protocolFactory.getProtocol(new TMemoryInputTransport(resBytes));
      TMessage msg = iprot.readMessageBegin();
      try {
        if (msg.type == TMessageType.EXCEPTION) {
          TException exception = TApplicationException.read(iprot);
          if (exception instanceof SourcedException) {
            if (this.serviceName != "") ((SourcedException) exception).serviceName_$eq(this.serviceName);
          }
          throw exception;
        } else {
          return codec.decode(iprot);
        }
      } finally {
        iprot.readMessageEnd();
      }
    }
  
    protected Exception missingResult(String name) {
      return new TApplicationException(
        TApplicationException.MISSING_RESULT,
        "`" + name + "` failed: unknown result"
      );
    }
  
    class __Stats {
      Counter requestsCounter, successCounter, failuresCounter;
      StatsReceiver failuresScope;
  
      public __Stats(String name) {
        StatsReceiver scope = FinagledClient.this.scopedStats.scope(name);
        this.requestsCounter = scope.counter0("requests");
        this.successCounter = scope.counter0("success");
        this.failuresCounter = scope.counter0("failures");
        this.failuresScope = scope.scope("failures");
      }
    }
  
    // ----- end boilerplate.
  
    private __Stats __stats_recommend = new __Stats("recommend");
  
    public Future<List<RecommendItemList>> recommend(RecommendParam param) {
      __stats_recommend.requestsCounter.incr();
  
      Future<List<RecommendItemList>> rv = this.service.apply(encodeRequest("recommend", new recommend_args(param))).flatMap(new Function<byte[], Future<List<RecommendItemList>>>() {
        public Future<List<RecommendItemList>> apply(byte[] in) {
          try {
            recommend_result result = decodeResponse(in, recommend_result.CODEC);
  
  
            if (result.success.isDefined()) return Future.value(result.success.get());
            return Future.exception(missingResult("recommend"));
          } catch (TException e) {
            return Future.exception(e);
          }
        }
      }).rescue(new Function<Throwable, Future<List<RecommendItemList>>>() {
        public Future<List<RecommendItemList>> apply(Throwable t) {
          if (t instanceof SourcedException) {
            ((SourcedException) t).serviceName_$eq(FinagledClient.this.serviceName);
          }
          return Future.exception(t);
        }
      });
  
      rv.addEventListener(new FutureEventListener<List<RecommendItemList>>() {
        public void onSuccess(List<RecommendItemList> result) {
          __stats_recommend.successCounter.incr();
        }
  
        public void onFailure(Throwable t) {
          __stats_recommend.failuresCounter.incr();
          __stats_recommend.failuresScope.counter0(t.getClass().getName()).incr();
        }
      });
  
      return rv;
    }
    private __Stats __stats_click = new __Stats("click");
  
    public Future<Void> click(ClickParam param) {
      __stats_click.requestsCounter.incr();
  
      Future<Void> rv = this.service.apply(encodeRequest("click", new click_args(param))).flatMap(new Function<byte[], Future<Void>>() {
        public Future<Void> apply(byte[] in) {
          try {
            click_result result = decodeResponse(in, click_result.CODEC);
  
  
            return Future.value(null);
          } catch (TException e) {
            return Future.exception(e);
          }
        }
      }).rescue(new Function<Throwable, Future<Void>>() {
        public Future<Void> apply(Throwable t) {
          if (t instanceof SourcedException) {
            ((SourcedException) t).serviceName_$eq(FinagledClient.this.serviceName);
          }
          return Future.exception(t);
        }
      });
  
      rv.addEventListener(new FutureEventListener<Void>() {
        public void onSuccess(Void result) {
          __stats_click.successCounter.incr();
        }
  
        public void onFailure(Throwable t) {
          __stats_click.failuresCounter.incr();
          __stats_click.failuresScope.counter0(t.getClass().getName()).incr();
        }
      });
  
      return rv;
    }
  }
  public static class FinagledService extends Service<byte[], byte[]> {
    final private FutureIface iface;
    final private TProtocolFactory protocolFactory;
  
    public FinagledService(final FutureIface iface, final TProtocolFactory protocolFactory) {
      this.iface = iface;
      this.protocolFactory = protocolFactory;
  
      addFunction("recommend", new Function2<TProtocol, Integer, Future<byte[]>>() {
        public Future<byte[]> apply(TProtocol iprot, final Integer seqid) {
          try {
            recommend_args args = recommend_args.decode(iprot);
            iprot.readMessageEnd();
            Future<List<RecommendItemList>> result;
            try {
              result = iface.recommend(args.param);
            } catch (Throwable t) {
              result = Future.exception(t);
            }
            return result.flatMap(new Function<List<RecommendItemList>, Future<byte[]>>() {
              public Future<byte[]> apply(List<RecommendItemList> value){
                return reply("recommend", seqid, new recommend_result.Builder().success(value).build());
              }
            }).rescue(new Function<Throwable, Future<byte[]>>() {
              public Future<byte[]> apply(Throwable t) {
                return Future.exception(t);
              }
            });
          } catch (TProtocolException e) {
            try {
              iprot.readMessageEnd();
              return exception("recommend", seqid, TApplicationException.PROTOCOL_ERROR, e.getMessage());
            } catch (Exception unrecoverable) {
              return Future.exception(unrecoverable);
            }
          } catch (Throwable t) {
            return Future.exception(t);
          }
        }
      });
      addFunction("click", new Function2<TProtocol, Integer, Future<byte[]>>() {
        public Future<byte[]> apply(TProtocol iprot, final Integer seqid) {
          try {
            click_args args = click_args.decode(iprot);
            iprot.readMessageEnd();
            Future<Void> result;
            try {
              result = iface.click(args.param);
            } catch (Throwable t) {
              result = Future.exception(t);
            }
            return result.flatMap(new Function<Void, Future<byte[]>>() {
              public Future<byte[]> apply(Void value){
                return reply("click", seqid, new click_result.Builder().build());
              }
            }).rescue(new Function<Throwable, Future<byte[]>>() {
              public Future<byte[]> apply(Throwable t) {
                return Future.exception(t);
              }
            });
          } catch (TProtocolException e) {
            try {
              iprot.readMessageEnd();
              return exception("click", seqid, TApplicationException.PROTOCOL_ERROR, e.getMessage());
            } catch (Exception unrecoverable) {
              return Future.exception(unrecoverable);
            }
          } catch (Throwable t) {
            return Future.exception(t);
          }
        }
      });
    }
  
    // ----- boilerplate that should eventually be moved into finagle:
  
    protected Map<String, Function2<TProtocol, Integer, Future<byte[]>>> functionMap =
      new HashMap<String, Function2<TProtocol, Integer, Future<byte[]>>>();
  
    protected void addFunction(String name, Function2<TProtocol, Integer, Future<byte[]>> fn) {
      functionMap.put(name, fn);
    }
  
    protected Function2<TProtocol, Integer, Future<byte[]>> getFunction(String name) {
      return functionMap.get(name);
    }
  
    protected Future<byte[]> exception(String name, int seqid, int code, String message) {
      try {
        TApplicationException x = new TApplicationException(code, message);
        TMemoryBuffer memoryBuffer = new TMemoryBuffer(512);
        TProtocol oprot = protocolFactory.getProtocol(memoryBuffer);
  
        oprot.writeMessageBegin(new TMessage(name, TMessageType.EXCEPTION, seqid));
        x.write(oprot);
        oprot.writeMessageEnd();
        oprot.getTransport().flush();
        return Future.value(Arrays.copyOfRange(memoryBuffer.getArray(), 0, memoryBuffer.length()));
      } catch (Exception e) {
        return Future.exception(e);
      }
    }
  
    protected Future<byte[]> reply(String name, int seqid, ThriftStruct result) {
      try {
        TMemoryBuffer memoryBuffer = new TMemoryBuffer(512);
        TProtocol oprot = protocolFactory.getProtocol(memoryBuffer);
  
        oprot.writeMessageBegin(new TMessage(name, TMessageType.REPLY, seqid));
        result.write(oprot);
        oprot.writeMessageEnd();
  
        return Future.value(Arrays.copyOfRange(memoryBuffer.getArray(), 0, memoryBuffer.length()));
      } catch (Exception e) {
        return Future.exception(e);
      }
    }
  
    public final Future<byte[]> apply(byte[] request) {
      TTransport inputTransport = new TMemoryInputTransport(request);
      TProtocol iprot = protocolFactory.getProtocol(inputTransport);
  
      try {
        TMessage msg = iprot.readMessageBegin();
        Function2<TProtocol, Integer, Future<byte[]>> f = functionMap.get(msg.name);
        if (f != null) {
          return f.apply(iprot, msg.seqid);
        } else {
          TProtocolUtil.skip(iprot, TType.STRUCT);
          return exception(msg.name, msg.seqid, TApplicationException.UNKNOWN_METHOD, "Invalid method name: '" + msg.name + "'");
        }
      } catch (Exception e) {
        return Future.exception(e);
      }
    }
  
    // ---- end boilerplate.
  }
}