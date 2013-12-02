#<< data/table

class data.ops.Partition extends data.Table

  constructor: (@table, @cols, @alias='table') ->
    super
    @cols = _.flatten [@cols]
    @schema = @table.schema.clone()#.project @cols
    @schema.addColumn @alias, data.Schema.table

  children: -> [@table]
  iterator: ->
    timer = @timer()
    class Iter
      constructor: (@schema, @table, @cols, @alias) ->
        @_row = new data.Row @schema
        @idx = 0
        timer.start()

      reset: -> 
        @idx = 0

      next: -> 
        throw Error("iterator has no more elements") unless @hasNext()
        htrow = @ht[@idx]
        @idx += 1
        @_row.reset()
        for col, idx in @cols
          @_row.set col, htrow.key[idx]
        filter = ((cols, truekey) ->
          (row) =>
            for col, idx in cols
              unless data.util.Util.isEqual row.get(col), truekey[idx]
                return no
            yes
        )(@cols, htrow.key)
        partitionf = new data.ops.Filter @table, filter
        partition = data.Table.fromArray htrow.table, @table.schema
        partition.children = => [@table]
        unless partitionf.nrows() == partition.nrows()
          console.log @cols
          console.log htrow.key
          console.log "#{partitionf.nrows()} vs #{partition.nrows()}"
          throw Error "filter based and array based partitions not same"
        if partition.nrows() > 0
          @_row.steal partition.any()
        @_row.set @alias, partitionf
        @_row

      hasNext: -> 
        unless @ht?
          @ht = _.values(data.ops.Util.buildHT @table, @cols)
        @idx < @ht.length

      close: -> 
        @table = null
        timer.stop()

    new Iter @schema, @table, @cols, @alias



