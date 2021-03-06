#<< data/table

#
# { 
#   col: colname
#   op: "<", ">", "<=", ">=", "=", "!=", 'in', 'of'
#   val: VALUE
# }
#
# {
#   col: colname
#   f: 
# }
#
# {
#   f:
# }
#
# f()
#
class data.ops.Filter extends data.Table
  constructor: (@table, @descs=[]) ->
    super
    @f = @constructor.normalizeDescs _.flatten([@descs])
    @schema = @table.schema
    @setProv()

  children: -> [@table]

  iterator: ->
    tid = @id
    timer = @timer()
    
    class Iter
      constructor: (@table, @f) ->
        @schema = @table.schema
        @iter = @table.iterator()
        @_next = null
        @_ret = new data.Row @schema
        @idx = 0

      reset: -> 
        @idx = 0
        @iter.reset()

      next: -> 
        throw Error("iterator has no more elements") unless @hasNext()?
        @idx += 1
        @_ret.reset()
        @_ret.steal @_next
        @_ret.id = data.Row.makeId tid, @idx-1
        @_next = null
        @_ret

      hasNext: -> 
        return true if @_next?
        while @iter.hasNext()
          timer.start()
          row = @iter.next()
          if @f row
            @_next = row
            timer.stop()
            break
          timer.stop()
        @_next?

      close: -> 
        @table = null
        @_ret = null
        @iter.close()

    new Iter @table, @f


  @normalizeDescs: (descs) ->
    descs = for desc in descs
      @normalizeDesc desc

    (row) ->
      for desc in descs
        unless desc.f(row)
          return no
      yes

  @normalizeDesc: (desc) ->
    if _.isFunction desc
      desc = {
        col: '*'
        f: desc
      }
    desc = _.clone desc
    
    if desc.col? and 'val' of desc
      desc.op ?= '='

    if desc.op? and desc.col? 
      desc.f = switch desc.op
        when '=' 
          cmd = "(row.get('#{desc.col}') == #{JSON.stringify desc.val})"
          Function("row", "return #{cmd}")
        when '<>'
          cmd = "(row.get('#{desc.col}') != #{JSON.stringify desc.val})"
          Function("row", "return #{cmd}")
        when 'in'
          ((col, val) ->
            (row) -> row.get(col) in val
          )(desc.col, desc.val)
        when 'of'
          ((col, val) ->
            (row) -> row.get(col) of val
          )(desc.col, desc.val)
        when 'between', 'btwn'
          lastIdx = desc.val.length-1
          cmd = "(row.get('#{desc.col}') >= #{desc.val[0]} && row.get('#{desc.col}') <= #{desc.val[1]})"
          Function("row", "return #{cmd}")
        else 
          cmd = "(row.get('#{desc.col}') #{desc.op} #{JSON.stringify desc.val})"
          Function("row", "return #{cmd}")
    else if desc.f?
      if desc.col? and desc.col != '*' 
        desc.f = ((f, col) ->
          (row) ->
            f row.get(col)
        )(desc.f, desc.col)
      else
        desc.col = '*'
    desc

  @validate: (schema, descs) ->
    for desc in descs
      if desc.col != '*' and not schema.has(desc.col)
        throw Error

