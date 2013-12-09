import java.io.IOException;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyModule;
import org.jruby.runtime.load.BasicLibraryService;

import autobahn.encoder.MsgPackLz4Encoder;


public class AutobahnMsgpackLz4Service implements BasicLibraryService {
  public boolean basicLoad(final Ruby runtime) throws IOException {
    RubyModule autobahnModule = runtime.getOrCreateModule("Autobahn");
    RubyClass encoderClass = autobahnModule.getClass("Encoder");
    RubyClass msgPackLz4Class = autobahnModule.defineClassUnder("MsgPackLz4Encoder", encoderClass, MsgPackLz4Encoder.ALLOCATOR);
    msgPackLz4Class.defineAnnotatedMethods(MsgPackLz4Encoder.class);
    return true;
  }
}

