$: << File.expand_path('../../../lib', __FILE__)


require 'bundler/setup'
require 'java'
require 'benchmark'
require 'autobahn'


class JavaGzipEncoder < Autobahn::Encoder
  import 'java.util.zip.GZIPOutputStream'
  import 'java.util.zip.GZIPInputStream'
  import 'java.io.ByteArrayOutputStream'
  import 'java.io.ByteArrayInputStream'

  content_encoding 'gzip'

  def encode(obj)
    baos = ByteArrayOutputStream.new
    gzos = GZIPOutputStream.new(baos)
    strb = @wrapped_encoder.encode(obj).to_java_bytes
    gzos.write(strb, 0, strb.length)
    gzos.close
    String.from_java_bytes(baos.to_byte_array)
  end

  def decode(str)
    bais = ByteArrayInputStream.new(str.to_java_bytes)
    gzis = GZIPInputStream.new(bais)
    output = ''.force_encoding(Encoding::BINARY)
    buffer = Java::byte[1024 * 16].new
    until gzis.available == 0
      bytes_read = gzis.read(buffer)
      if bytes_read > 0
        output << String.from_java_bytes(buffer)[0, bytes_read]
      end
    end
    gzis.close
    @wrapped_encoder.decode(output)
  end
end

require 'zlib'
require 'stringio'

class ZlibGzipEncoder < Autobahn::Encoder
  content_encoding 'gzip'

  def encode(obj)
    io = StringIO.new
    gz = Zlib::GzipWriter.new(io)
    gz.print(@wrapped_encoder.encode(obj))
    gz.close
    io.string
  end

  def decode(str)
    io = StringIO.new(str)
    gz = Zlib::GzipReader.new(io)
    @wrapped_encoder.decode(gz.read).tap { gz.close }
  end
end

n = 10_000

data = DATA.read
 
java_gzip = JavaGzipEncoder.new(Autobahn::StringEncoder.new)
zlib_gzip = ZlibGzipEncoder.new(Autobahn::StringEncoder.new)

encoded_data = zlib_gzip.encode(data)

Benchmark.bm(16) do |x|
  x.report('Java Gzip Encode') { n.times { java_gzip.encode(data) } }
  x.report('Zlib Gzip Encode') { n.times { zlib_gzip.encode(data) } }
  x.report('Java Gzip Decode') { n.times { java_gzip.decode(encoded_data) } }
  x.report('Zlib Gzip Decode') { n.times { zlib_gzip.decode(encoded_data) } }
end

__END__
{"agent_version":"1175-JS","client_timestamp":7114,"media_key":"STMPBQZTZ5GL","user_id":"M4NMNTD1NTQK","website_name":"gp.se","click_enabled":true,"media_type":["DIV"],"node_count":1,"page_dimension":[1344,10321],"placement_kind":"absolute","placement_name":"eniro-sponsored-links-element-rightcol","script_access":true,"session_id":"M4NMO0PHC71S","node_visibility":[0],"timestamp":1338076800821,"ip":"62.107.17.94","platform":"RFM","pageview_ref":"M4NMNTQ47OT80000","category":"frontpage","embed_url":"http://www.gp.se/","canonical_path":"www.gp.se/","referrer_domain":"mediavejviseren.dk","sample_rate":1,"last_timestamp":1338076800821,"last_client_timestamp":26790,"pre_crunched":true,"media_rating":0.0,"average_visibility":0,"browser":"Internet Explorer","device_type":"computer","has_activity_updates":true,"engagement":false,"engagement_duration":0,"time_to_engagement":0,"redirect_url":null,"click":false,"time_to_click_thru":0,"has_error":false,"ad_orientation":"square","ad_dimensions":"301x242","share_of_view":0.0,"external_ids":{},"valid":true}
{"agent_version":"1175-JS","client_timestamp":7117,"media_key":"STMPBQZTZ5GL","user_id":"M4NMNTD1NTQK","website_name":"gp.se","click_enabled":false,"wrapper_url":"http://biowebb-data.s3.amazonaws.com/pages/12/city/56/date/today/index.html","media_type":["IFRAME"],"node_count":1,"page_dimension":[1344,10321],"placement_kind":"absolute","placement_name":"sfannonsen","script_access":true,"session_id":"M4NMO0RAF203","node_visibility":[0],"timestamp":1338076800821,"ip":"62.107.17.94","platform":"RFM","pageview_ref":"M4NMNTQ47OT80000","category":"frontpage","embed_url":"http://www.gp.se/","canonical_path":"www.gp.se/","referrer_domain":"mediavejviseren.dk","sample_rate":1,"last_timestamp":1338076800821,"last_client_timestamp":26790,"pre_crunched":true,"media_rating":0.0,"average_visibility":0,"browser":"Internet Explorer","device_type":"computer","has_activity_updates":true,"engagement":false,"engagement_duration":0,"time_to_engagement":0,"redirect_url":null,"click":false,"time_to_click_thru":0,"has_error":false,"ad_orientation":"square","ad_dimensions":"1000x1000","share_of_view":0.0,"external_ids":{},"valid":true}
{"agent_version":"1175-JS","client_timestamp":2180,"media_key":"STMPBQZTZ5GL","user_id":"LU3RNWLG54MH","website_name":"gp.se","click_enabled":true,"load_url":"http://fusion.adtoma.com/11C883A096B/11C88E6CA70.jpg","media_type":["IMG"],"node_count":1,"page_dimension":[1280,10173],"placement_kind":"absolute","placement_name":"helsida_1","script_access":true,"session_id":"M4NMO0P6UQWX","tag_id":"122554817110","node_visibility":[0],"timestamp":1338076803103,"ip":"217.211.1.119","platform":"RFM","pageview_ref":"M4NMO041HOZL0000","category":"frontpage","embed_url":"http://www.gp.se/","canonical_path":"www.gp.se/","sample_rate":1,"last_timestamp":1338076803103,"last_client_timestamp":2530,"pre_crunched":true,"media_rating":0.0,"average_visibility":0,"browser":"Chrome","device_type":"computer","has_activity_updates":true,"engagement":false,"engagement_duration":0,"time_to_engagement":0,"redirect_url":null,"click":false,"time_to_click_thru":0,"has_error":false,"ad_orientation":"square","ad_dimensions":"440x400","share_of_view":0.0,"external_ids":{},"valid":true,"ideal_revenue":0.152245258,"currency":"SEK","has_ideal_revenue?":true}
{"agent_version":"1175-JS","client_timestamp":2166,"media_key":"STMPBQZTZ5GL","user_id":"LU3RNWLG54MH","website_name":"gp.se","click_enabled":true,"load_url":"http://media.dealie.se/dealie/goteborg/210x300","media_type":["IMG"],"node_count":1,"page_dimension":[1280,10173],"placement_kind":"absolute","placement_name":"dealie","script_access":true,"session_id":"M4NMO2816F4X","node_visibility":[0],"timestamp":1338076803035,"ip":"217.211.1.119","platform":"RFM","pageview_ref":"M4NMO041HOZL0000","category":"frontpage","embed_url":"http://www.gp.se/","canonical_path":"www.gp.se/","sample_rate":1,"last_timestamp":1338076803035,"last_client_timestamp":2530,"pre_crunched":true,"media_rating":0.0,"average_visibility":0,"browser":"Chrome","device_type":"computer","has_activity_updates":true,"engagement":false,"engagement_duration":0,"time_to_engagement":0,"redirect_url":null,"click":false,"time_to_click_thru":0,"has_error":false,"ad_orientation":"portrait","ad_dimensions":"210x300","share_of_view":0.0,"external_ids":{},"valid":true}
{"agent_version":"1175-JS","client_timestamp":2165,"media_key":"STMPBQZTZ5GL","user_id":"LU3RNWLG54MH","website_name":"gp.se","click_enabled":false,"wrapper_url":"http://biowebb-data.s3.amazonaws.com/pages/12/city/56/date/today/index.html","media_type":["IFRAME"],"node_count":1,"page_dimension":[1280,10173],"placement_kind":"absolute","placement_name":"sfannonsen","script_access":true,"session_id":"M4NMO2SN6RAL","node_visibility":[0],"timestamp":1338076803035,"ip":"217.211.1.119","platform":"RFM","pageview_ref":"M4NMO041HOZL0000","category":"frontpage","embed_url":"http://www.gp.se/","canonical_path":"www.gp.se/","sample_rate":1,"last_timestamp":1338076803035,"last_client_timestamp":2530,"pre_crunched":true,"media_rating":0.0,"average_visibility":0,"browser":"Chrome","device_type":"computer","has_activity_updates":true,"engagement":false,"engagement_duration":0,"time_to_engagement":0,"redirect_url":null,"click":false,"time_to_click_thru":0,"has_error":false,"ad_orientation":"square","ad_dimensions":"1000x1000","share_of_view":0.0,"external_ids":{},"valid":true}
{"agent_version":"1175-JS","client_timestamp":1685,"media_key":"STMPBQZTZ5GL","user_id":"M4NMO5VVS0EN","website_name":"gp.se","click_enabled":false,"load_url":"http://www.gp.se/polopoly_fs/1.954757.1337948308!/image/1632830087.png","media_type":["IMG","IMG","IMG","IMG","IMG","IMG","IMG","IMG"],"node_count":1,"page_dimension":[1450,10173],"placement_kind":"absolute","placement_name":"uppslag","script_access":true,"session_id":"M4NMO5CITC8X","tag_id":"122556064100","node_visibility":[0,0,0,0,0,0,0,0],"timestamp":1338076808617,"ip":"83.233.2.132","platform":"RFM","pageview_ref":"M4NMO5PSBAC70000","category":"frontpage","embed_url":"http://www.gp.se/","canonical_path":"www.gp.se/","sample_rate":1,"last_timestamp":1338076808617,"last_client_timestamp":12815,"pre_crunched":true,"media_rating":0.0,"average_visibility":0,"browser":"Safari","device_type":"computer","has_activity_updates":true,"engagement":false,"engagement_duration":0,"time_to_engagement":0,"redirect_url":null,"click":false,"time_to_click_thru":0,"has_error":false,"ad_orientation":"landscape","ad_dimensions":"434x72","share_of_view":0.0,"external_ids":{},"valid":true,"ideal_revenue":0.166127072,"currency":"SEK","has_ideal_revenue?":true}
{"agent_version":"1175-JS","client_timestamp":1374,"media_key":"STMPBQZTZ5GL","user_id":"M4NMO5VVS0EN","website_name":"gp.se","click_enabled":true,"load_url":"http://media.dealie.se/dealie/goteborg/210x300","media_type":["IMG"],"node_count":1,"page_dimension":[1450,10173],"placement_kind":"absolute","placement_name":"dealie","script_access":true,"session_id":"M4NMO6TST0J3","node_visibility":[0],"timestamp":1338076808364,"ip":"83.233.2.132","platform":"RFM","pageview_ref":"M4NMO5PSBAC70000","category":"frontpage","embed_url":"http://www.gp.se/","canonical_path":"www.gp.se/","sample_rate":1,"last_timestamp":1338076808364,"last_client_timestamp":12815,"pre_crunched":true,"media_rating":0.0,"average_visibility":0,"browser":"Safari","device_type":"computer","has_activity_updates":true,"engagement":false,"engagement_duration":0,"time_to_engagement":0,"redirect_url":null,"click":false,"time_to_click_thru":0,"has_error":false,"ad_orientation":"portrait","ad_dimensions":"210x300","share_of_view":0.0,"external_ids":{},"valid":true}
{"agent_version":"1175-JS","client_timestamp":1801,"media_key":"STMPBQZTZ5GL","user_id":"LWKMZ1UTN51Z","website_name":"gp.se","click_enabled":true,"media_type":["DIV"],"node_count":1,"page_dimension":[1279,10173],"placement_kind":"absolute","placement_name":"eniro-sponsored-links-element-rightcol","script_access":true,"session_id":"M4NMO5B41DYJ","node_visibility":[0],"timestamp":1338076805429,"ip":"77.110.16.158","platform":"RFM","pageview_ref":"M4NMO3IBL7770000","category":"frontpage","embed_url":"http://www.gp.se/","canonical_path":"www.gp.se/","sample_rate":1,"last_timestamp":1338076805429,"last_client_timestamp":6357,"pre_crunched":true,"media_rating":0.0,"average_visibility":0,"browser":"Safari","device_type":"computer","has_activity_updates":true,"engagement":false,"engagement_duration":0,"time_to_engagement":0,"redirect_url":null,"click":false,"time_to_click_thru":0,"has_error":false,"ad_orientation":"square","ad_dimensions":"302x253","share_of_view":0.0,"external_ids":{},"valid":true}
{"agent_version":"1175-JS","client_timestamp":1930,"media_key":"STMPBQZTZ5GL","user_id":"LWKMZ1UTN51Z","website_name":"gp.se","click_enabled":false,"load_url":"http://www.gp.se/polopoly_fs/1.954837.1337949932!/image/2762152958.jpg","media_type":["IMG","IMG","IMG","IMG","IMG","IMG","IMG","IMG"],"node_count":1,"page_dimension":[1279,10173],"placement_kind":"absolute","placement_name":"uppslag","script_access":true,"session_id":"M4NMO3GOB69R","tag_id":"122556064140","node_visibility":[0,0,0,0,0,0,0,0],"timestamp":1338076805898,"ip":"77.110.16.158","platform":"RFM","pageview_ref":"M4NMO3IBL7770000","category":"frontpage","embed_url":"http://www.gp.se/","canonical_path":"www.gp.se/","sample_rate":1,"last_timestamp":1338076805898,"last_client_timestamp":6357,"pre_crunched":true,"media_rating":0.0,"average_visibility":0,"browser":"Safari","device_type":"computer","has_activity_updates":true,"engagement":false,"engagement_duration":0,"time_to_engagement":0,"redirect_url":null,"click":false,"time_to_click_thru":0,"has_error":false,"ad_orientation":"landscape","ad_dimensions":"440x135","share_of_view":0.0,"external_ids":{},"valid":true,"ideal_revenue":0.166127072,"currency":"SEK","has_ideal_revenue?":true}
{"agent_version":"1175-JS","client_timestamp":1804,"media_key":"STMPBQZTZ5GL","user_id":"LWKMZ1UTN51Z","website_name":"gp.se","click_enabled":false,"wrapper_url":"http://biowebb-data.s3.amazonaws.com/pages/12/city/56/date/today/index.html","media_type":["IFRAME"],"node_count":1,"page_dimension":[1279,10173],"placement_kind":"absolute","placement_name":"sfannonsen","script_access":true,"session_id":"M4NMO50A2A3C","node_visibility":[0],"timestamp":1338076805429,"ip":"77.110.16.158","platform":"RFM","pageview_ref":"M4NMO3IBL7770000","category":"frontpage","embed_url":"http://www.gp.se/","canonical_path":"www.gp.se/","sample_rate":1,"last_timestamp":1338076805429,"last_client_timestamp":6357,"pre_crunched":true,"media_rating":0.0,"average_visibility":0,"browser":"Safari","device_type":"computer","has_activity_updates":true,"engagement":false,"engagement_duration":0,"time_to_engagement":0,"redirect_url":null,"click":false,"time_to_click_thru":0,"has_error":false,"ad_orientation":"square","ad_dimensions":"1000x1000","share_of_view":0.0,"external_ids":{},"valid":true}
