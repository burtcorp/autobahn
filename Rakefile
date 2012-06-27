$: << 'lib'

require 'autobahn/version'


task :release do
  version_string = "v#{Autobahn::VERSION}"
  unless %x(git tag -l).include?(version_string)
    system %(git tag -a #{version_string} -m #{version_string})
  end
  system %(git push && git push --tags)
  system %(gem build autobahn.gemspec && gem inabox autobahn-*.gem && mv autobahn-*.gem pkg)
end