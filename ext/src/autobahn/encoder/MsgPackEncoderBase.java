package autobahn.encoder;


import java.io.IOException;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyObject;
import org.jruby.RubyHash;
import org.jruby.RubyString;
import org.jruby.runtime.builtin.IRubyObject;
import org.jruby.runtime.ObjectAllocator;
import org.jruby.runtime.ThreadContext;
import org.jruby.anno.JRubyMethod;
import org.jruby.anno.JRubyClass;
import org.jruby.util.ByteList;

import static org.jruby.runtime.Visibility.*;

import com.headius.jruby.lz4.vendor.net.jpountz.lz4.LZ4Compressor;
import com.headius.jruby.lz4.vendor.net.jpountz.lz4.LZ4Decompressor;
import com.headius.jruby.lz4.vendor.net.jpountz.lz4.LZ4Factory;

import org.msgpack.jruby.Encoder;
import org.msgpack.jruby.Decoder;


abstract public class MsgPackEncoderBase extends RubyObject {
  private static final String CONTENT_TYPE = "application/msgpack";

  private final RubyString contentType;
  private final RubyString contentEncoding;
  private final RubyHash properties;
  private boolean symbolizeKeys;
  private boolean compatibilityMode;

  public MsgPackEncoderBase(Ruby runtime, RubyClass type, RubyString contentEncoding) {
    super(runtime, type);
    this.contentType = runtime.newString(CONTENT_TYPE);
    this.contentEncoding = contentEncoding;
    this.properties = RubyHash.newHash(runtime);
    this.properties.put(runtime.newSymbol("content_type"), contentType);
    this.properties.put(runtime.newSymbol("content_encoding"), contentEncoding);
  }

  @JRubyMethod(name = "initialize", optional = 1, visibility = PRIVATE)
  public IRubyObject initialize(ThreadContext ctx, IRubyObject[] args) {
    this.symbolizeKeys = false;
    this.compatibilityMode = false;
    if (args.length == 1) {
      RubyHash options = args[0].convertToHash();
      IRubyObject symbolizeKeysValue = options.fastARef(ctx.getRuntime().newSymbol("symbolize_keys"));
      IRubyObject compatibilityModeValue = options.fastARef(ctx.getRuntime().newSymbol("compatibility_mode"));
      this.symbolizeKeys = symbolizeKeysValue != null && symbolizeKeysValue.isTrue();
      this.compatibilityMode = compatibilityModeValue != null && compatibilityModeValue.isTrue();
    }
    return this;
  }

  @JRubyMethod(required = 1)
  public IRubyObject encode(ThreadContext ctx, IRubyObject obj) throws IOException {
    Encoder encoder = new Encoder(ctx.getRuntime(), compatibilityMode);
    return compress(ctx.getRuntime(), encoder.encode(obj).asString().getByteList());
  }

  protected abstract RubyString compress(Ruby runtime, ByteList packed) throws IOException;

  @JRubyMethod(required = 1)
  public IRubyObject decode(ThreadContext ctx, IRubyObject str) throws IOException {
    ByteList compressed = str.asString().getByteList();
    Decoder decoder = new Decoder(ctx.getRuntime(), decompress(compressed));
    decoder.symbolizeKeys(symbolizeKeys);
    if (decoder.hasNext()) {
      return decoder.next();
    } else {
      return ctx.getRuntime().getNil();
    }
  }

  abstract protected byte[] decompress(ByteList compressed) throws IOException;

  @JRubyMethod(name = "properties")
  public IRubyObject getProperties(ThreadContext ctx) {
    return properties;
  }

  @JRubyMethod(name = "encodes_batches?")
  public IRubyObject getEncodesBatches(ThreadContext ctx) {
    return ctx.getRuntime().getTrue();
  }

 @JRubyMethod(name = "content_type", module = true)
 public static IRubyObject getContentType(ThreadContext ctx, IRubyObject recv) {
   return ctx.getRuntime().newString(CONTENT_TYPE);
 }
}
