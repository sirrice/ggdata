class data.ops.Cross extends data.Table
  constructor: (@left, @right, @jointype, @leftf=null, @rightf=null) ->
    super
    @schema = @left.schema.clone()
    @schema.merge @right.schema.clone()
    @setup()


  setup: ->
    @timer().start 'setup'
    defaultf = -> new data.Row(new data.Schema)
    @leftf ?= defaultf
    @rightf ?= defaultf
    liter = @left.iterator()
    riter = @right.iterator()
    lhasRows = liter.hasNext()
    rhasRows = riter.hasNext()
    liter.close()
    riter.close()

    switch @jointype
      when "left"
        unless rhasRows
          @right = data.Table.fromArray([@rightf()], @right.schema) 
          
      when "right"
        unless lhasRows
          tmp = @left
          @left = @right
          @right = tmp
          @right = data.Table.fromArray([@leftf()], @left.schema) 

      when "outer"
        unless lhasRows
          tmp = @left
          @left = @right
          @right = tmp
          @right = data.Table.fromArray([@leftf()], @left.schema) 
        else unless rhasRows
          @right = data.Table.fromArray([@rightf()], @right.schema) 

    @timer().stop 'setup'

  nrows: -> @left.nrows() * @right.nrows()
  children: -> [@left, @right]
          
  iterator: ->
    timer = @timer()
    class Iter
      constructor: (@schema, @left, @right) ->
        @_row = new data.Row @schema
        @liter = @left.iterator()
        @riter = @right.cache().iterator()
        @lrow = null
        @reset()
        timer.start()

      reset: ->
        @liter.reset()
        @riter.reset()

      next: ->
        throw Error("iterator has no more elements") unless @hasNext()
        @_row.steal(@lrow).steal(@riter.next())

      hasNext: ->
        @lrow = null unless @riter.hasNext()

        while @liter.hasNext() and not(@lrow? and @riter.hasNext())
          timer.end 'innerloop'
          @riter.reset()
          @lrow = @liter.next()
          timer.start 'innerloop'

        @lrow? and @riter.hasNext()

      close: ->
        @left = @right = null
        @liter.close()
        @riter.close()
        timer.stop()

    new Iter @schema, @left, @right


