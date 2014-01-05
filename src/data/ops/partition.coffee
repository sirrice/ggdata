#<< data/table

# Equi-partition on a set of columns
#
class data.ops.Partition extends data.Table

  # @param alias attribute name for the partition table
  constructor: (@table, @partcols, @alias='table') ->
    super
    @partcols = _.flatten [@partcols]
    # clone schema so we copy nonpartition cols like fill
    # TODO: fix this
    @schema = @table.schema.clone() # .project @partcols
    @schema.addColumn @alias, data.Schema.table
    @ht = null
    @setProv()


  children: -> [@table]
  iterator: ->
    timer = @timer()
    _me = @
    tid = @id

    class Iter
      constructor: (@schema, @table, @cols, @alias) ->
        @_row = new data.Row @schema
        @idx = 0

      reset: -> 
        @idx = 0

      next: -> 
        throw Error("iterator has no more elements") unless @hasNext()
        timer.start()
        htrow = _me.ht[@idx]
        @idx += 1
        @_row.reset()
        @_row.id = data.Row.makeId tid, @idx-1
        
        for col, idx in @cols
          @_row.set col, htrow.key[idx]

        partitionf = new data.ops.Array(
          @table.schema,
          htrow.rows,
          [@table]
        )

        if partitionf.nrows() > 0
          @_row.steal partitionf.any()
        @_row.set @alias, partitionf

        for row in htrow.rows
          @_row.addProv row.prov()

        timer.stop()
        @_row

      hasNext: -> 
        unless _me.ht?
          timer.start()
          _me.ht = _.values(data.ops.Util.buildHT _me.table, _me.partcols)
          timer.stop()
        @idx < _me.ht.length

      close: -> 
        @table = null

    new Iter @schema, @table, @partcols, @alias



