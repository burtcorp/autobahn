import java.io.IOException;

import org.jruby.Ruby;
import org.jruby.RubyClass;
import org.jruby.RubyModule;
import org.jruby.runtime.load.BasicLibraryService;

import autobahn.encoder.MsgPackLzfEncoder;
import autobahn.encoder.MsgPackEncoderBase;


public class AutobahnMsgpackLzfService implements BasicLibraryService {
  public boolean basicLoad(final Ruby runtime) throws IOException {
    RubyModule autobahnModule = runtime.getOrCreateModule("Autobahn");
    RubyClass encoderClass = autobahnModule.getClass("Encoder");
    RubyClass msgPackLzfClass = autobahnModule.defineClassUnder("MsgPackLzfEncoder", encoderClass, MsgPackLzfEncoder.ALLOCATOR);
    msgPackLzfClass.defineAnnotatedMethods(MsgPackEncoderBase.class);
    msgPackLzfClass.defineAnnotatedMethods(MsgPackLzfEncoder.class);
    return true;
  }
}

