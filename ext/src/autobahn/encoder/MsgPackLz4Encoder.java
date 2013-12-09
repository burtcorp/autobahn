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

import com.headius.jruby.lz4.vendor.net.jpountz.lz4.LZ4Compressor;
import com.headius.jruby.lz4.vendor.net.jpountz.lz4.LZ4Decompressor;
import com.headius.jruby.lz4.vendor.net.jpountz.lz4.LZ4Factory;


@JRubyClass(name="Autobahn::MsgPackLz4Encoder")
public class MsgPackLz4Encoder extends RubyObject {
  private static final LZ4Factory LZ4_FACTORY = LZ4Factory.fastestInstance();

  private final LZ4Compressor compressor;
  private final LZ4Decompressor decompressor;
  private final MessagePack msgPack;
  private final RubyObjectPacker packer;
  private final RubyObjectUnpacker unpacker;
  private final RubyHash properties;
  private RubyHash unpackerOptions;

  public MsgPackLz4Encoder(Ruby runtime, RubyClass type) {
    super(runtime, type);
    this.compressor = LZ4_FACTORY.fastCompressor();
    this.decompressor = LZ4_FACTORY.decompressor();
    this.msgPack = new MessagePack();
    this.packer = new RubyObjectPacker(msgPack);
    this.unpacker = new RubyObjectUnpacker(msgPack);
    this.properties = RubyHash.newHash(runtime);
    this.properties.put(runtime.newSymbol("content_type"), runtime.newString("application/msgpack"));
    this.properties.put(runtime.newSymbol("content_encoding"), runtime.newString("lz4"));
  }

  @JRubyMethod(name = "initialize", optional = 1, visibility = PRIVATE)
  public IRubyObject initialize(ThreadContext ctx, IRubyObject[] args) {
    unpackerOptions = (args.length == 1 && args[0] instanceof RubyHash) ? (RubyHash) args[0] : null;
    return this;
  }

  @JRubyMethod(required = 1)
  public IRubyObject encode(ThreadContext ctx, IRubyObject obj) throws IOException {
    byte[] packed = packer.packRaw(obj);
    int maxBufferSize = 4 + compressor.maxCompressedLength(packed.length);
    byte[] compressed = new byte[maxBufferSize];
    int headerSize = encodeHeader(packed.length, compressed);
    int compressedSize = compressor.compress(packed, 0, packed.length, compressed, headerSize, maxBufferSize - headerSize);
    return RubyString.newStringNoCopy(ctx.getRuntime(), compressed, 0, headerSize + compressedSize);
  }

  private int encodeHeader(int size, byte[] buffer) {
    int headerSize = 0;
    while (true) {
      int b = size & 0x7f;
      size >>= 7;
      if (size == 0) {
        buffer[headerSize] = (byte) b;
        return ++headerSize;
      } else {
        buffer[headerSize] = (byte) (b | 0x80);
        ++headerSize;
      }
    }
  }

  @JRubyMethod(required = 1)
  public IRubyObject decode(ThreadContext ctx, IRubyObject str) throws IOException {
    byte[] compressed = str.asString().getBytes();
    int[] header = decodeHeader(compressed);
    byte[] packed = new byte[header[1]];
    decompressor.decompress(compressed, header[0], packed, 0, header[1]);
    return unpacker.unpack(ctx.getRuntime(), packed, unpackerOptions);
  }

  private int[] decodeHeader(byte[] buffer) {
    int uncompressedSize = 0;
    int headerSize = 0;

    for (int i = 0; i < 4; i++) {
      uncompressedSize |= (buffer[i] & 0x7f) << (7 * i);
      ++headerSize;
      if ((buffer[i] & 0x80) == 0) {
        return new int[] {headerSize, uncompressedSize};
      }
    }
    return new int[] {-1, -1};
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
      return new MsgPackLz4Encoder(runtime, type);
    }
  };
}