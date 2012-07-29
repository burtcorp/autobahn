# encoding: utf-8

$: << 'lib'

require 'bundler/setup'
require 'autobahn/version'
require 'ant'


namespace :release do
  task :tag do
    version_string = "v#{Autobahn::VERSION}"
    unless %x(git tag -l).include?(version_string)
      system %(git tag -a #{version_string} -m #{version_string})
    end
    system %(git push && git push --tags)
  end

  task :gem => ['build:clean', 'build:jars'] do
    system %(gem build autobahn.gemspec && gem inabox autobahn-*.gem && mv autobahn-*.gem pkg)
  end
end

task :release => ['release:tag', 'release:gem']

namespace :build do
  source_dir = 'ext/src'
  build_dir = 'ext/build'

  directory build_dir

  task :setup => build_dir do
    ant.property :name => 'src.dir', :value => source_dir
    ant.path :id => 'compile.class.path' do
      pathelement :location => File.join(ENV['MY_RUBY_HOME'], 'lib', 'jruby.jar')
      Dir['lib/ext/*.jar'].each do |jar|
        pathelement :location => jar unless jar.start_with?('autobahn')
      end
      $LOAD_PATH.flat_map { |path| Dir[File.join(path, '**', '*.jar')] }.each do |jar|
        pathelement :location => jar
      end
    end
  end

  task :compile => :setup do
    ant.javac :destdir => build_dir, :includeantruntime => 'no', :target => '1.6', :source => '1.6', :debug => 'on' do
      classpath :refid => 'compile.class.path'
      src { pathelement :location => '${src.dir}' }
    end
  end

  task :jars => :compile do
    ant.jar :destfile => 'lib/autobahn_msgpack_lzf.jar', :basedir => build_dir do
      ant.fileset :dir => build_dir, :includes => 'autobahn/encoder/MsgPackLzfEncoder*.class'
      ant.fileset :dir => build_dir, :includes => 'AutobahnMsgpackLzfService.class'
    end
  end

  task :clean do
    rm_rf build_dir
    rm Dir['lib/autobahn*.jar']
  end
end

task :build => 'build:jars'