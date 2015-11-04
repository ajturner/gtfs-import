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
require 'rubygems'
require 'zip'
require 'concurrent'
require 'arcgis-ruby'
require 'pry'


class GTFSImport
  # Define the list of files that comprise a GTFS zip
  REQUIRED_FILES = [
    "agency.txt",
    "stops.txt",
    "routes.txt",
    "trips.txt",
    "stop_times.txt",
    "calendar.txt"
  ]

  OPTIONAL_FILES = [
    "calendar_dates.txt",
    "fare_attributes.txt",
    "fare_rules.txt",
    "shapes.txt",
    "frequencies.txt",
    "transfers.txt",
    "feed_info.txt",
    "stops.txt"
  ]


  #
  # Kick off the import process--a group may optionally be passed in to receive
  # the files, otherwise one will be created with the name "GTFS Import"
  #
  def self.import(config)
    dir = Dir.mktmpdir

    begin
      # binding.pry
      files = extract_files(zip_file: config["file"], dir: dir)

      valid = (REQUIRED_FILES - files.map{|f| f[:file_name]}).empty?
      raise "Invalid GTFS format. No files were uploaded." unless valid

      # Strip out nonstandard files
      files = files.select{|f| (REQUIRED_FILES + OPTIONAL_FILES).include?(f[:file_name])}

      # Begin making the appropriate API calls
      # TODO: pull this from config!
      connection = Arcgis::Connection.new(
        host: config["host"],
        username: config["username"],
        password: config["password"]
      )

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

      # Create a kml file for a map using the transitfeed python library
      requests += kml_item(connection: connection, group_id: group_id, dir: dir, file: config["file"])

      # Set up ArcGIS requests for raw files
      files.each do |item|
        args = {connection: connection, item: item, group_id: group_id}
        requests += simple_item(args)
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


  #
  # This creates a kml file based on the GTFS data, courtesy of the transitfeed
  # library from google. This script should ideally be refactored in Python to
  # not have to resort to a system call.
  #
  def self.kml_item(file:, group_id:, connection:, dir:)
    r_create_kml = Concurrent::dataflow do
      puts "Generating KML"
      FileUtils.cp(file, "#{dir}/gtfs.zip")
      system "./transitfeed/kmlwriter.py #{dir}/gtfs.zip #{dir}/gtfs.kml >/dev/null"
    end

    r_create = Concurrent::dataflow(r_create_kml) do |_|
      puts "Creating KML item"
      connection.user.add_item(title: "gtfs.kml", type: "KML", tags: "gtfs",
        file: File.open("#{dir}/gtfs.kml"))
    end

    r_share = Concurrent::dataflow(r_create) do |created_item|
      puts "Sharing KML"
      connection.item(created_item["id"]).share(groups: group_id,
        everyone: true, org: true)
    end
    
    [r_create_kml, r_create, r_share]

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
        {
          name: file.name.gsub('.txt','').split('_').map(&:capitalize).join(' '),
          file_name: file.name,
          path: path
        }
      end
    end
  end

end

GTFSImport.import(YAML.load("config.yml"))
