#<< data/table

class data.ops.Flatten extends data.Table
  constructor: (@table) ->
    @schema = @table.schema
    tablecols = _.filter @schema.cols, (col) =>
      @schema.type(col) == data.Schema.table
    @tablecol = tablecol = tablecols[0]
    othercols =  _.reject @schema.cols, (col) =>
      @schema.type(col) == data.Schema.table
    otherSchema = @schema.project othercols

    if @table.nrows() > 0
      row = @table.any()
      p = row.get tablecol
      @schema = otherSchema.merge p.schema

    super

    @timer().start()
    newtables = @table.map (row) =>
      lefto = _.o2map othercols, (col) ->
        [col, row.get(col)]
      left = new data.ops.Array otherSchema, [lefto], [@table]
      right = row.get(tablecol).cache()
      left.cross right
    @timer().stop()

    @iter = new data.ops.Union newtables

  nrows: -> @iter.nrows()
  children: -> [@table]

  iterator: ->
    class Iter
      constructor: (@schema, @table, @tablecol) ->
        @iter = @table.iterator()
        @piter = null
        @currow = null
        @_row = new data.Row @schema

      reset: ->
        @piter.reset() if @piter?
        @iter.reset()

      next: ->
        throw Error unless @hasNext()
        @_row.reset()
        #@_row.steal @currow
        @_row.steal @piter.next()
        @_row

      hasNext: ->
        return yes if @piter? and @piter.hasNext()

        if @piter? 
          @piter.close()
          @piter = null

        while @iter.hasNext()
          @currow = @iter.next()
          p = @currow.get(@tablecol)
          continue unless p?
          @piter = p.iterator()
          break if @piter.hasNext()
    
        @piter? and @piter.hasNext()

      close: ->
        @piter.close() if @piter?
        @iter.close()

    new Iter @schema, @table, @tablecol
