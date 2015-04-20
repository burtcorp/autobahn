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
public class MsgPackLzfEncoder extends MsgPackEncoderBase {
  private static final RubyString CONTENT_ENCODING = Ruby.getGlobalRuntime().newString("lzf");

  public MsgPackLzfEncoder(Ruby runtime, RubyClass type) {
    super(runtime, type, CONTENT_ENCODING);
  }

  @JRubyMethod(name = "content_encoding", module = true)
  public static IRubyObject getContentEncoding(ThreadContext ctx, IRubyObject recv) {
    return CONTENT_ENCODING;
  }

  @Override
  protected RubyString compress(Ruby runtime, ByteList packed) throws IOException {
    byte[] compressed = LZFEncoder.encode(packed.unsafeBytes(), packed.begin(), packed.length());
    return RubyString.newStringNoCopy(runtime, compressed, 0, compressed.length);
  }

  @Override
  protected byte[] decompress(ByteList compressed) throws IOException {
    return LZFDecoder.decode(compressed.unsafeBytes(), compressed.begin(), compressed.length());
  }

  public static final ObjectAllocator ALLOCATOR = new ObjectAllocator() {
    public IRubyObject allocate(Ruby runtime, RubyClass type) {
      return new MsgPackLzfEncoder(runtime, type);
    }
  };
}