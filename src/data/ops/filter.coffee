#<< data/table

#
# { 
#   col: colname
#   op: "<", ">", "<=", ">=", "=", "!="
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
    @f = @constructor.normalizeDescs _.flatten([@descs])

    @schema = @table.schema
    super

  children: -> [@table]
  @normalizeDescs: (descs) ->
    descs = for desc in descs
      @normalizeDesc desc

    (row) ->
      for desc in descs
        unless desc.f(row)
          return no
      yes

  @normalizeDesc: ( desc) ->
    if _.isFunction desc
      desc = {
        col: '*'
        f: desc
      }
    
    if desc.col? and 'val' of desc
      desc.op ?= '='

    if desc.op? and desc.col? 
      desc.op = switch desc.op
        when '=' then '=='
        else desc.op
      cmd = "(row.get('#{desc.col}') #{desc.op} #{JSON.stringify desc.val})"
      desc.f = Function("row", "return #{cmd}")
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

  iterator: ->
    timer = @timer()
    class Iter
      constructor: (@table, @f) ->
        @schema = @table.schema
        @iter = @table.iterator()
        @_next = null

      reset: -> @iter.reset()
      next: -> 
        throw Error("iterator has no more elements") unless @hasNext()?
        ret = @_next
        @_next = null
        ret

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
        @iter.close()

    new Iter @table, @f



