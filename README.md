# GTFS Importer

This script will import a GTFS zip file as specified here:
https://developers.google.com/transit/gtfs/reference?hl=en

It will create an ArcGIS item for each required or optional file provided,
and a feature service for stops.txt (see PUBLISH_STEP_ACTIONS in code). It
will ignore any files that are not listed in the specification. It will then
mark each created item as public, open data.

## Requirements
The following gems are required: zip, concurrent, arcgis-ruby.

## Usage
The Config module contains all of the parts you need to change. Note that you
can leave GROUP_ID as nil if you want the script to automatically create a
group for you.

After that, simply run it from the command line! It should take less than a
minute to run.

## Transit Feed
This uses the https://github.com/google/transitfeed library for KML generation,
so a big thank you to Google for providing it!
