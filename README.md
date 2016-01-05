# GTFS Importer

This script will import a GTFS zip file as specified here: https://developers.google.com/transit/gtfs/reference?hl=en

It will create an ArcGIS item for each required or optional file provided, and a feature service for stops.txt (see PUBLISH_STEP_ACTIONS in code). It will ignore any files that are not listed in the specification. It will then mark each created item as public, open data.

It will also create a feature service containing two layers: one with the stops and one with the routes. Each of these layers work fine individually, but don't show up on the feature service map itself for some reason. Assistance debugging this would be most appreciated.

## Requirements
The following gems are required: zip, concurrent, arcgis-ruby.

## Usage
config.yml module contains all of the parts you need to change. Note that you can leave group_id as nil if you want the script to automatically create a group for you.

After you've configured config.yml, simply run it from the command line!

```bash
cd /path/to/script
ruby gtfs.rb
```
