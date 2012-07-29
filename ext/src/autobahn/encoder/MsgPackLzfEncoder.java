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
  private final MessagePack msgPack;
  private final RubyObjectUnpacker unpacker;
  private final RubyHash properties;

  public MsgPackLzfEncoder(Ruby runtime, RubyClass type) {
    super(runtime, type);
    this.msgPack = new MessagePack();
    this.unpacker = new RubyObjectUnpacker(msgPack);
    this.properties = RubyHash.newHash(runtime);
    this.properties.put(runtime.newSymbol("content_type"), runtime.newString("application/msgpack"));
    this.properties.put(runtime.newSymbol("content_encoding"), runtime.newString("lzf"));
  }

  @JRubyMethod(name = "initialize", visibility = PRIVATE)
  public IRubyObject initialize(ThreadContext ctx) {
    return this;
  }

  public byte[] encodeRaw(IRubyObject obj) throws IOException {
    // TODO: move into msgpack-jruby, make a method that returns byte[]
    BufferPacker bufferedPacker = msgPack.createBufferPacker();
    Packer packer = new RubyObjectPacker(msgPack, bufferedPacker).write(obj);
    byte[] packed = bufferedPacker.toByteArray();
    byte[] compressed = LZFEncoder.encode(packed);
    return compressed;
  }

  @JRubyMethod(required = 1)
  public IRubyObject encode(ThreadContext ctx, IRubyObject obj) throws IOException {
    return RubyString.newString(ctx.getRuntime(), encodeRaw(obj));
  }

  @JRubyMethod(required = 1)
  public IRubyObject decode(ThreadContext ctx, IRubyObject str) throws IOException {
    // TODO: move into msgpack-jruby, make a method that takes byte[]
    byte[] compressed = str.asString().getBytes();
    byte[] packed = LZFDecoder.decode(compressed);
    return unpacker.unpack(ctx.getRuntime(), packed);
  }

  @JRubyMethod(name = "properties")
  public IRubyObject getProperties(ThreadContext ctx) {
    return properties;
  }

  @JRubyMethod(name = "encodes_batches?")
  public IRubyObject getEncodesBatches(ThreadContext ctx) {
    return ctx.getRuntime().getTrue();
  }

  public static final ObjectAllocator ALLOCATOR = new ObjectAllocator() {
    public IRubyObject allocate(Ruby runtime, RubyClass type) {
      return new MsgPackLzfEncoder(runtime, type);
    }
  };
}