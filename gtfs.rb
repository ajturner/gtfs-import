# /usr/bin/env ruby
#
# GTFS IMPORTER
#
# This script will import a GTFS zip file as specified here:
# https://developers.google.com/transit/gtfs/reference?hl=en
#
# It will create an ArcGIS item for each required or optional file provided,
# and a feature service for stops.txt (see PUBLISH_STEP_ACTIONS in code). It
# will ignore any files that are not listed in the specification. It will then
# mark each created item as public, open data.
#
# Requirements:
# The following gems are required: zip, concurrent, arcgis-ruby.
#
# Usage:
# Copy the config.example.yml to config.yml and fill in the correct details for
# your use case.
#
# After that, simply run it from the command line! It should take less than a
# minute to run, depending on data size and connection speed.
#

require 'yaml'
require 'uri'
require 'rubygems'
require 'zip'
require 'concurrent'
require 'arcgis-ruby'
require 'pry'
require 'csv'
require 'optparse'

config = YAML.load(File.read("#{File.dirname(__FILE__)}/config.yml"))
Options = Struct.new(:name)

class Parser
  def self.parse(options)
    args = Hash.new()

    opt_parser = OptionParser.new do |opts|
      opts.banner = "Usage: gtfs.rb [options]"

      opts.on("-u USERNAME", "--username=USERNAME", "ArcGIS Username") do |n|
        args["username"] = n
      end

      opts.on("-p PASSWORD", "--password=PASSWORD", "ArcGIS Password") do |n|
        args["password"] = n
      end

      opts.on("-f FILENAME", "--file=FILENAME", "Path to GTFS Zipfile") do |n|
        args["file"] = n
      end

      opts.on("-n NAME", "--name=NAME", "Name of the Service") do |n|
        args["service_name"] = n
      end

      opts.on("-g GROUPID", "--group=GROUPID", "ArcGIS Group to save GTFS services. Blank for new group") do |n|
        args["group_id"] = n
      end

      opts.on("-d ARCGISURL", "--domain=ARCGISURL", "URL to the ArcGIS Portal. Default http://arcgis.com") do |n|
        args["host"] = n
      end

      opts.on("-h", "--help", "Prints this help") do
        puts opts
        exit
      end
    end

    opt_parser.parse!(options)
    return args
  end
end
options = Parser.parse ARGV #%w[--help]
config.merge!(options)

class GTFSImport
  # Define the list of files that comprise a GTFS zip
  REQUIRED_FILES = [
    "agency.txt",
    "stops.txt",
    "routes.txt",
    "trips.txt",
    "stop_times.txt"
  ]

  OPTIONAL_FILES = [
    "calendar.txt",
    "calendar_dates.txt",
    "fare_attributes.txt",
    "fare_rules.txt",
    "shapes.txt",
    "frequencies.txt",
    "transfers.txt",
    "feed_info.txt",
    "stops.txt"
  ]

  PUBLISH_STEP_ACTIONS = {
    "stops.txt" => {
      "locationType" => "coordinates",
      "latitudeFieldName" => "stop_lat",
      "longitudeFieldName" => "stop_lon"
    },

    "shapes.txt" => {
      "locationType" => "coordinates",
      "latitudeFieldName" => "shape_pt_lat",
      "longitudeFieldName" => "shape_pt_lon"
    },
  }

  DEFAULT_SHAPE_COLOR = 'AAAAAA'

  #
  # Kick off the import process--a group may optionally be passed in to receive
  # the files, otherwise one will be created with the name "GTFS Import"
  #
  def self.import(config)
    dir = Dir.mktmpdir

    begin
      files = extract_files(zip_file: config["file"], dir: dir)

      missing_files = REQUIRED_FILES - files.map{|f| f[:file_name]}
      valid = missing_files.empty?
      raise "Invalid GTFS format. No files were uploaded. (missing #{missing_files.join(',')})" unless valid

      # Strip out nonstandard files
      files = files.select{|f| (REQUIRED_FILES + OPTIONAL_FILES).include?(f[:file_name])}

      # Begin making the appropriate API calls
      connection = Arcgis::Connection.new(
        host: config["host"],
        username: config["username"],
        password: config["password"]
      )

      # connection.search(q: config["service_name"])["results"].map do |x|
      #   Concurrent::Future.execute{ connection.item(x["id"]).delete rescue nil }
      # end.each{|x| x.value}

      # 2.times do
      #   connection.search(q: "tags:gtfs")["results"].map do |x|
      #     Concurrent::Future.execute{ connection.item(x["id"]).delete rescue nil }
      #   end.each{|x| x.value}
      # end

      # Create a new group if necessary
      group_id = config["group_id"] || begin
        puts "Creating GTFS Group"
        group = connection.group.create(
          title: "GTFS Import",
          access: "account",
          description: "An import of GTFS data"
        )
        group["group"]["id"]
      end

      requests = []

      args = {
        connection: connection,
        files: files,
        group_id: group_id,
        config: config
      }
      requests += create_service(args)

      # Set up ArcGIS requests for raw files
      files.each do |item|
        args = {connection: connection, item: item, group_id: group_id}
        if !PUBLISH_STEP_ACTIONS[item[:file_name]]
          # requests += simple_item(args)
        end
      end

      # The created requests are run concurrently. Block on each until they've
      # all been run. If we hit any errors, we'll throw them at the end.
      errors = []
      requests.each do |r|
        r.value
        if r.rejected?
          errors << r.reason
        end
      end

      if errors.empty?
        puts "Everything has been imported successfully. Yay!"
      else
        raise errors.join("\n")
      end

    ensure
      FileUtils.remove_entry dir
    end
  end


  #
  # Called for files that just get uploaded as is.
  #
  def self.simple_item(connection:, item:, group_id:)
    r_create = Concurrent::dataflow do
      puts "Creating #{item[:name]}"
      connection.user.add_item(title: item[:name], type: "CSV", tags: "gtfs",
        file: File.open(item[:path])) 
    end

    r_share = Concurrent::dataflow(r_create) do |created_item|
      puts "Sharing #{item[:name]}"
      connection.item(created_item["id"]).share(groups: group_id,
        everyone: true, org: true)
    end

    [r_create, r_share]
  end


  def self.create_service(connection:, files:, group_id:, config:)
    stops_item = files.detect{|f| f[:name] == 'Stops'}
    shapes_item = files.detect{|f| f[:name] == 'Shapes'}
    agency_item = files.detect{|f| f[:name] == 'Agency'}

    agency_csv = CSV::parse(File.read(agency_item[:path]), headers: true)
    agency_name = config['service_name'].to_s.empty? ? agency_csv.first["agency_name"] : config['service_name']

    # create a service
    # r_service = Concurrent::dataflow do
    service = begin
      service_tmpl = File.read("#{File.dirname(__FILE__)}/service_template.json")
      service_json_str = service_tmpl.gsub('{{agency_name}}', agency_name)
      service_json = JSON.parse(service_json_str)

      puts "Creating feature service: #{agency_name}"
      service = connection.user.create_service(service_json)

      # AGOL chrome
      fs_uri = URI.parse(service["encodedServiceURL"])
      fs_admin_conn = Arcgis::Connection.new(
        host: "https://#{fs_uri.host}#{fs_uri.path.split('/')[0..3].join('/')}",
        username: connection.instance_eval('@username'),
        token: connection.instance_eval('@token')
      )
      fs_name = fs_uri.path.split('/')[-2]
      add_layer_path = "/admin/services/#{fs_name}/FeatureServer/addToDefinition"

      # Stops Layer
      layers = [JSON.parse(File.read("#{File.dirname(__FILE__)}/stops_layer_template.json"))]

      # Routes layer if we have shapes.txt
      if shapes_item
        colors = shape_colors(files) # strange stuff--only works if invoked twice
        shape_symbols = colors.map do |shape,color|
          { value: shape,
            label: shape,
            symbol: {
              color: color,
              width: 3.75,
              type: "esriSLS",
              style: "esriSLSSolid"
            }}
        end

        shape_types = colors.map do |shape,color|
          { id: shape,
            name: shape,
            templates: [{
              name: shape,
              drawingTool: "esriFeatureEditToolNone",
              prototype: {
                attributes: {
                  shape_id: shape
          }}}]}
        end

        shapes_json_tmpl = File.read("#{File.dirname(__FILE__)}/shapes_layer_template.json")
        shapes_json_str = shapes_json_tmpl.
          gsub('{{uniqueValueInfos}}', shape_symbols.to_json).
          gsub('{{types}}', shape_types.to_json)

        shapes_json = JSON.parse(shapes_json_str)

        layers << shapes_json
      end

      layers_temp = {addToDefinition: {layers: layers}}
      fs_admin_conn.run(path: add_layer_path, method: "POST", body: layers_temp)

      service
    end

    stops = begin
      stops_csv = CSV::parse(File.read(stops_item[:path]))
      stops_header = stops_csv.shift

      add_features_path = "/0/addFeatures"
      fs_conn = Arcgis::Connection.new(
        host: service["encodedServiceURL"],
        username: connection.instance_eval('@username'),
        token: connection.instance_eval('@token')
      )

      puts "Analyzing stops"
      analysis = connection.feature.analyze(file: File.open(stops_item[:path]), filetype: "csv")
      r_stop_analyses = stops_csv.each_slice(1000).to_a.map do |slice|
        Concurrent::Future.execute do
          csv_data = ([stops_header] + slice)
          text = CSV::generate{|x| csv_data.each{|y| x << y}}
          params = {
            filetype: "csv",
            text: text,
            publishParameters: analysis["publishParameters"].
              merge(PUBLISH_STEP_ACTIONS[stops_item[:file_name]])
          }

          connection.run(path: '/content/features/generate', method: 'POST', body: params)
        end
      end

      r_stop_analyses.each_with_index do |r_slice, idx|
        slice = r_slice.value
        s_stops = slice["featureCollection"]["layers"].first["featureSet"]["features"]

        features = s_stops.map do |stop|
          { geometry: stop["geometry"],
            attributes: {
              stop_name: stop["attributes"]["stop_name"],
              stop_lat: stop["attributes"]["stop_lat"],
              stop_lon: stop["attributes"]["stop_lon"],
              'FID' => stop["attributes"]["FID"]
          }}
        end

        pct = (((idx*1000).to_f/stops_csv.size) * 100).floor
        puts "Adding stops to feature service (#{pct}%)"
        fs_conn.run(path: add_features_path, method: "POST", body: {features: features})
      end
    end
    
    # share
    # r_share = Concurrent::dataflow(r_service, r_shapes, r_stops) do |service,_1,_2|
    share = begin
      puts "Sharing feature service"
      connection.item(service["serviceItemId"]).share(
        groups: group_id, everyone: true, org: true
      )
    end


    # r_shapes = Concurrent::dataflow(r_service) do |service|
    shapes = begin
      add_features_path = "/1/addFeatures"
      fs_conn = Arcgis::Connection.new(
        host: service["encodedServiceURL"],
        username: connection.instance_eval('@username'),
        token: connection.instance_eval('@token')
      )

      shapes_csv = CSV::parse(File.read(shapes_item[:path]), headers: true)

      # translate the coordinates, doing it all at once to minimize requests
      coordinates = shapes_csv.map do |shape|
        [shape["shape_pt_lat"], shape["shape_pt_lon"]]
      end
      translated = translate_coordinates(coordinates, connection)

      shape_groups = shapes_csv.
        zip(translated).
        reduce({}){|memo,(s,coordinates)|
          x = memo[s["shape_id"]] || []
          elt = {
            "shape_id" => s["shape_id"],
            "coordinates" => coordinates,
            "sequence" => s["shape_pt_sequence"].to_i
          }

          memo.merge({s["shape_id"] => (x + [elt])})
        }

      puts "generating features"
      features = shape_groups.values.map do |group|
        sample = group.first
        { attributes: {shape_id: sample["shape_id"], "FID" => sample["shape_id"]},
          geometry: {
            paths: [ group.
              sort{|a,b| a["sequence"] <=> b["sequence"]}.
              map{|g| g["coordinates"]}],
            spatialReference: {wkid: 102100, latestWkid: 3857}
          }
        }
      end

      puts "Adding routes feature"
      fs_conn.run(path: add_features_path, method: "POST", body: {features: features})
    end



    # [r_service, r_stops, r_shapes, r_share]
    []
  end


  #
  # Create a dictionary from shape_id to route_color
  #
  def self.shape_colors(files)
    routes_item = files.detect{|f| f[:name] == 'Routes'}
    trips_item = files.detect{|f| f[:name] == 'Trips'}
    routes_csv = CSV.read(routes_item[:path], headers: true)
    trips_csv = CSV.read(trips_item[:path], headers: true)

    to_dec = ->(x){ [x[0..1].to_i(16), x[2..3].to_i(16), x[4..5].to_i(16), 255] }

    route_colors = routes_csv.reduce({}) do |memo,route|
      color = route["route_color"].to_s.empty? ? "888888" : route["route_color"] # Grey if not defined
      memo.merge({route["route_id"] => to_dec[color]})
    end

    trips_csv.reduce({}) do |memo,trip|
      memo.merge({trip["shape_id"] => route_colors[trip["route_id"]]})
    end
  end


  #
  # Transform an array of lat/lon pairs into 102100 coordinates
  #
  def self.translate_coordinates(coordinates, connection)
    shim_csv = ->(data) { CSV::generate {|csv|
      csv << ["lat","lon"]
      data.each{|x| csv << x}
    }}

    analysis = connection.feature.analyze(text: shim_csv[coordinates], filetype: "csv")

    reqs = coordinates.each_slice(1000).to_a.map do |slice|
      # Concurrent::Future.execute do
        params = {
          filetype: 'csv',
          text: shim_csv[slice],
          publishParameters: analysis["publishParameters"].merge(
            "locationType" => "coordinates",
            "latitudeFieldName" => "lat",
            "longitudeFieldName" => "lon"
          )
        }
        connection.run(path: '/content/features/generate', method: 'POST', body: params)
      # end
    end

    reqs.flat_map do |res|
      # res.value["featureCollection"]["layers"].first["featureSet"]["features"].map do |data|
      res["featureCollection"]["layers"].first["featureSet"]["features"].map do |data|
        [data["geometry"]["x"], data["geometry"]["y"]]
      end
    end
  end


  #
  # Extract files from the GTFS zip file
  #
  def self.extract_files(zip_file:, dir:)
    Zip::File.open(zip_file) do |zip|
      zip.map do |file|
        path = "#{dir}/#{file.name}"
        out = File.open(path, 'w')
        out.write file.get_input_stream.read.force_encoding("UTF-8")
        out.close

        {
          name: file.name.gsub('.txt','').split('_').map(&:capitalize).join(' '),
          file_name: file.name,
          path: path
        }
      end
    end
  end

end

GTFSImport.import(config)
