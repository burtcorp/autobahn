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

import com.ning.compress.lzf.LZFEncoder;
import com.ning.compress.lzf.LZFDecoder;

import org.msgpack.jruby.Encoder;
import org.msgpack.jruby.Decoder;


@JRubyClass(name="Autobahn::MsgPackLzfEncoder")
public class MsgPackLzfEncoder extends RubyObject {
  private static final RubyString CONTENT_TYPE = Ruby.getGlobalRuntime().newString("application/msgpack");
  private static final RubyString CONTENT_ENCODING = Ruby.getGlobalRuntime().newString("lzf");

  private final Encoder encoder;
  private final RubyHash properties;
  private RubyHash unpackerOptions;

  public MsgPackLzfEncoder(Ruby runtime, RubyClass type) {
    super(runtime, type);
    this.encoder = new Encoder(runtime);
    this.properties = RubyHash.newHash(runtime);
    this.properties.put(runtime.newSymbol("content_type"), CONTENT_TYPE);
    this.properties.put(runtime.newSymbol("content_encoding"), CONTENT_ENCODING);
  }

  @JRubyMethod(name = "initialize", optional = 1, visibility = PRIVATE)
  public IRubyObject initialize(ThreadContext ctx, IRubyObject[] args) {
    unpackerOptions = (args.length == 1 && args[0] instanceof RubyHash) ? (RubyHash) args[0] : null;
    return this;
  }

  @JRubyMethod(required = 1)
  public IRubyObject encode(ThreadContext ctx, IRubyObject obj) throws IOException {
    ByteList packed = encoder.encode(obj).asString().getByteList();
    byte[] compressed = LZFEncoder.encode(packed.unsafeBytes(), packed.begin(), packed.length());
    return RubyString.newStringNoCopy(ctx.getRuntime(), compressed, 0, compressed.length);
  }

  @JRubyMethod(required = 1)
  public IRubyObject decode(ThreadContext ctx, IRubyObject str) throws IOException {
    ByteList compressed = str.asString().getByteList();
    byte[] packed = LZFDecoder.decode(compressed.unsafeBytes(), compressed.begin(), compressed.length());
    Decoder decoder = new Decoder(ctx.getRuntime(), packed);
    if (decoder.hasNext()) {
      return decoder.next();
    } else {
      return ctx.getRuntime().getNil();
    }
  }

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
    return CONTENT_TYPE;
  }

  @JRubyMethod(name = "content_encoding", module = true)
  public static IRubyObject getContentEncoding(ThreadContext ctx, IRubyObject recv) {
    return CONTENT_ENCODING;
  }

  public static final ObjectAllocator ALLOCATOR = new ObjectAllocator() {
    public IRubyObject allocate(Ruby runtime, RubyClass type) {
      return new MsgPackLzfEncoder(runtime, type);
    }
  };
}