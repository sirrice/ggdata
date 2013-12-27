#<< data/table

class data.ops.Freeze extends data.Table
  constructor: (@table) ->
    super
    @schema = @table.schema
    @frozen = yes
    @setProv()

  children: -> [@table]
  melt: -> @table
  iterator: -> @table.iterator()