module ShapesFunctions
  class << self
    def parse(data)
      data.map do |datum|
        {
          id: datum[0],
          shape_pt_lat: datum[1].to_f,
          shape_pt_lon: datum[2].to_f,
          shape_pt_sequence: datum[3].to_i,
          shape_dist_traveled: datum[4].to_f
        }
      end
    end

    def construct_lines(shapes_data) # array of lines
      lines = parse(shapes_data).
        group_by({|d| d[:id]}).
        sort({|a,b| a[:shape_pt_sequence] <=> b[:shape_pt_sequence]})

      lines.map do |line|
        [line[:shape_pt_lat], line[:shape_pt_lon]]
      end
    end
  end
end
