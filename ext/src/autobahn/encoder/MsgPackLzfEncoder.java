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

import static org.jruby.runtime.Visibility.*;

import org.msgpack.jruby.RubyObjectPacker;
import org.msgpack.jruby.RubyObjectUnpacker;

import org.msgpack.MessagePack;
import org.msgpack.packer.BufferPacker;
import org.msgpack.packer.Packer;

import com.ning.compress.lzf.LZFEncoder;
import com.ning.compress.lzf.LZFDecoder;


@JRubyClass(name="Autobahn::MsgPackLzfEncoder")
public class MsgPackLzfEncoder extends RubyObject {
  private static final RubyString CONTENT_TYPE = Ruby.getGlobalRuntime().newString("application/msgpack");
  private static final RubyString CONTENT_ENCODING = Ruby.getGlobalRuntime().newString("lzf");

  private final MessagePack msgPack;
  private final RubyObjectPacker packer;
  private final RubyObjectUnpacker unpacker;
  private final RubyHash properties;
  private RubyHash unpackerOptions;

  public MsgPackLzfEncoder(Ruby runtime, RubyClass type) {
    super(runtime, type);
    this.msgPack = new MessagePack();
    this.packer = new RubyObjectPacker(msgPack);
    this.unpacker = new RubyObjectUnpacker(msgPack);
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
    byte[] packed = packer.packRaw(obj);
    byte[] compressed = LZFEncoder.encode(packed);
    return RubyString.newString(ctx.getRuntime(), compressed);
  }

  @JRubyMethod(required = 1)
  public IRubyObject decode(ThreadContext ctx, IRubyObject str) throws IOException {
    byte[] compressed = str.asString().getBytes();
    byte[] packed = LZFDecoder.decode(compressed);
    return unpacker.unpack(ctx.getRuntime(), packed, unpackerOptions);
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