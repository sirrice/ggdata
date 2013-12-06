#<< data/table
#<< data/ops/array


class data.ops.Cache extends data.ops.Array
  constructor: (@table) ->
    rows = @setup()
    super @table.schema, rows, [@table]

  setup: ->
    timer = @timer()
    timer.start()
    tablecols = _.filter @table.schema.cols, (col) =>
      @table.schema.type(col) == data.Schema.table
    rows = @table.map (row) ->
      row = row.clone()
      for col in tablecols
        if row.get(col)?
          row.set col, row.get(col).cache()
      row
    timer.stop()
    rows
