package autobahn.encoder;


import java.io.IOException;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyModule;
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


@JRubyClass(name="Autobahn::MsgPackLz4Encoder")
public class MsgPackLz4Encoder extends MsgPackEncoderBase {
  private static final LZ4Factory LZ4_FACTORY = LZ4Factory.fastestInstance();
  private static final String CONTENT_ENCODING = "lz4";

  private final LZ4Compressor compressor;
  private final LZ4Decompressor decompressor;

  public MsgPackLz4Encoder(Ruby runtime, RubyClass type) {
    super(runtime, type, runtime.newString(CONTENT_ENCODING));
    this.compressor = LZ4_FACTORY.fastCompressor();
    this.decompressor = LZ4_FACTORY.decompressor();
  }

  @JRubyMethod(name = "content_encoding", module = true)
  public static IRubyObject getContentEncoding(ThreadContext ctx, IRubyObject recv) {
    return ctx.getRuntime().newString(CONTENT_ENCODING);
  }

  protected RubyString compress(Ruby runtime, ByteList packed) throws IOException {
    int maxBufferSize = 4 + compressor.maxCompressedLength(packed.length());
    byte[] compressed = new byte[maxBufferSize];
    int headerSize = encodeHeader(packed.length(), compressed);
    int compressedSize = compressor.compress(packed.unsafeBytes(), packed.begin(), packed.length(), compressed, headerSize, maxBufferSize - headerSize);
    return RubyString.newStringNoCopy(runtime, compressed, 0, headerSize + compressedSize);
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

  protected byte[] decompress(ByteList compressed) throws IOException {
    byte[] compressedBytes = compressed.unsafeBytes();
    int compressedOffset = compressed.begin();
    int compressedLength = compressed.length();
    int[] header = decodeHeader(compressedBytes, compressedOffset, compressedLength);
    byte[] packed = new byte[header[1]];
    decompressor.decompress(compressedBytes, header[0], packed, compressedOffset, header[1]);
    return packed;
  }

  private int[] decodeHeader(byte[] buffer, int offset, int length) {
    int uncompressedSize = 0;
    int headerSize = 0;

    for (int i = offset; i < offset + 4; i++) {
      uncompressedSize |= (buffer[i] & 0x7f) << (7 * i);
      ++headerSize;
      if ((buffer[i] & 0x80) == 0) {
        return new int[] {headerSize, uncompressedSize};
      }
    }
    return new int[] {-1, -1};
  }

  public static final ObjectAllocator ALLOCATOR = new ObjectAllocator() {
    public IRubyObject allocate(Ruby runtime, RubyClass type) {
      return new MsgPackLz4Encoder(runtime, type);
    }
  };

  public static void load(Ruby runtime) {
    RubyModule autobahnModule = runtime.getOrCreateModule("Autobahn");
    RubyClass encoderClass = autobahnModule.getClass("Encoder");
    RubyClass msgPackLz4Class = autobahnModule.defineClassUnder("MsgPackLz4Encoder", encoderClass, MsgPackLz4Encoder.ALLOCATOR);
    msgPackLz4Class.defineAnnotatedMethods(MsgPackEncoderBase.class);
    msgPackLz4Class.defineAnnotatedMethods(MsgPackLz4Encoder.class);
  }
}