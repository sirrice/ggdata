require "../env"
vows = require "vows"
assert = require "assert"


suite = vows.describe "table.js"
Table = data.Table
Schema = data.Schema

makeTable = (n=10, type="row") ->
  rows = _.times n, (i) -> {
    a: i%2, 
    b: "#{i}"
    x: i
    y: {
      z: i
    }
  }
  Table.fromArray rows, null, type

checks = (nrows) ->

  "cached":
    topic: (t) ->
      t.cache()

    "has correct # rows": (t) ->
      assert.equal t.nrows(), nrows

  "once": ->
    topic: (t) ->
      t.once()

    "does not have _arraytable set": (t) ->
      assert (not t._arraytable?)

    "after reading it once": 
      topic: (t) ->
        t.all()
        t

      "has _arraytable": (t) ->
        assert t._arraytable?

      "reading again doesn't crash": (t) ->
        t.all()


  "filter x < 4":
    "using {col:, val:}":
      topic: (t) ->
        t.filter {col: 'x', op: '<', val: 4}
      "has 4 rows": (t) ->
        assert.equal t.nrows(), 4
        t.each (row, idx) ->
          assert.equal row.get('x'), idx

    "using {col:, f:}":
      topic: (t) ->
        t.filter {col: 'x', f: ((x)->x<4)}
      "has 4 rows": (t) ->
        assert.equal t.nrows(), 4
        t.each (row, idx) ->
          assert.equal row.get('x'), idx


    "using f":
      topic: (t) ->
        t.filter (row) -> row.get('x') < 4
      "has 4 rows": (t) ->
        assert.equal t.nrows(), 4
        t.each (row, idx) ->
          assert.equal row.get('x'), idx

    "using multiple filters":
      topic: (t) ->
        t.filter [
          ((row) -> row.get('x') < 4),
          {
            col: 'x', f: ((x)->x<4)
          },
          {
            col: 'x', op: '<', val: 4
          }
        ]
      "has 4 rows": (t) ->
        assert.equal t.nrows(), 4
        t.each (row, idx) ->
          assert.equal row.get('x'), idx


  "distinct on a":
    topic: (t) -> t.distinct 'a'
    "has 2 rows": (t) ->
      assert.equal t.nrows(), 2
  
  "partition on a":
    topic: (t) -> t.partition(['a'])
    "has 2 rows": (t) ->
      assert.equal t.nrows(), 2

    "single function aggregate c=count(x), s=sum(x)": 
      topic: (t) ->
        t.aggregate [
          {
            alias: ['c', 's']
            col: 'a'
            f: (vals) ->
              {
                c: vals.length
                s: _.reduce(vals, ((a,b)->a+b), 0)
              }
            type: data.Schema.numeric
          }
        ]
      "is correct": (t) ->
        t.each (row) ->
          #assert.equal row.get('s'), row.get('a')*nrows/2
          assert.equal row.get('c'), nrows/2

    "multi function aggregate c=count(x), s=sum(x)": 
      topic: (t) ->
        t.aggregate [
          data.ops.Aggregate.count 'c'
          data.ops.Aggregate.sum 'a'
        ]
      "is correct": (t) ->
        t.each (row) ->
          assert.equal row.get('c'), nrows/2

    "unioned together":
      topic: (t) ->
        new data.ops.Union t.all('table')

      "ordered by x": 
        topic: (t) ->
          t.orderby 'x'

        "is correct": (t) ->
          assert.equal t.nrows(), nrows
          t.each (row, idx) ->
            assert.equal row.get('x'), idx, "x was #{row.get 'x'} and not #{idx}"

    "cached":
      topic: (t) -> 
        t.cache()

      "table cols are intact": (t) ->
        t.each (row, idx) ->
          assert _.isType(row.get('table'), data.Table)

    "once":
      topic: (t) ->
        t.once()

      "table cols are intact": (t) ->
        t.each (row, idx) ->
          assert _.isType(row.get('table'), data.Table)

      "after iterated once": 
        topic: (t) -> 
          t.all()
          t

        "has n rows": (t) ->
          assert.equal t.nrows(), 2

        "table cols are intact": (t) ->
          t.each (row, idx) ->
            assert _.isType(row.get('table'), data.Table)


        "unioned together":
          topic: (t) ->
            new data.ops.Union t.all('table')

          "ordered by x": 
            topic: (t) ->
              t.orderby 'x'

            "is correct": (t) ->
              assert.equal t.nrows(), nrows
              t.each (row, idx) ->
                assert.equal row.get('x'), idx, "x was #{row.get 'x'} and not #{idx}"



  "union with itself":
    topic: (t) -> t.union t
    "has 20 rows": (t) ->
      assert.equal t.nrows(), nrows*2

  "union with itself 5 times as array":
    topic: (t) -> t.union _.times(5, ()->t)
    "has 60 rows": (t) ->
      assert.equal t.nrows(), 6*nrows


  "union with itself 5 times as args":
    topic: (t) -> t.union t,t,t,t,t
    "has 60 rows": (t) ->
      assert.equal t.nrows(), 6*nrows


  "limit 2":
    topic: (t) -> t.limit 2
    "has 2 rows": (t) ->
      assert.equal t.nrows(), 2
      t.each (row) ->
        assert.lt row.get('x'), 3

  "offset 5 limit 2":
    topic: (t) -> t.offset(5).limit 2
    "has 2 rows": (t) ->
      assert.equal t.nrows(), 2
    "5 <= x < 7": (t) ->
      t.each (row) ->
        assert.lte 5, row.get('x')
        assert.lt row.get('x'), 7

  "other table  { x: i+5, t: i%3}":
    topic: (t) ->
      rows = _.times nrows, (i) -> { x: i+5, b: i%3}
      t2 = data.Table.fromArray rows
      [t, t2]

    "is cached": ->
      topic: ([t, t2]) -> [t.cache(), t2.cache()]

      "cross product via .cross()":
        topic: ([t1, t2]) -> t1.cross t2
        "has 100 rows": (t) ->
          assert.equal t.nrows(), nrows*nrows


      "cross product via join":
        topic: ([t1, t2]) -> t1.join t2, []
        "has 100 rows": (t) ->
          assert.equal t.nrows(), nrows*nrows

      "outer join on x":
        topic: ([t1, t2]) -> t1.join t2, ['x']
        "has 15 rows": (t) ->
          assert.equal t.nrows(), 5+nrows

      "left join on x":
        topic: ([t1, t2]) -> t1.join t2, ['x'], "left"
        "has 10 rows": (t) ->
          assert.equal t.nrows(), nrows

      "right join on x":
        topic: ([t1, t2]) -> t1.join t2, ['x'], "right"
        "has 10 rows": (t) ->
          assert.equal t.nrows(), nrows

      "inner join on x":
        topic: ([t1, t2]) -> t1.join t2, ['x'], "inner"
        "has 10 rows": (t) ->
          assert.equal t.nrows(), nrows - 5


    "cross product via .cross()":
      topic: ([t1, t2]) -> t1.cross t2
      "has 100 rows": (t) ->
        assert.equal t.nrows(), nrows*nrows


    "cross product via join":
      topic: ([t1, t2]) -> t1.join t2, []
      "has 100 rows": (t) ->
        assert.equal t.nrows(), nrows*nrows

    "outer join on x":
      topic: ([t1, t2]) -> t1.join t2, ['x']
      "has 15 rows": (t) ->
        assert.equal t.nrows(), 5+nrows

    "left join on x":
      topic: ([t1, t2]) -> t1.join t2, ['x'], "left"
      "has 10 rows": (t) ->
        assert.equal t.nrows(), nrows

    "right join on x":
      topic: ([t1, t2]) -> t1.join t2, ['x'], "right"
      "has 10 rows": (t) ->
        assert.equal t.nrows(), nrows

    "inner join on x":
      topic: ([t1, t2]) -> t1.join t2, ['x'], "inner"
      "has 10 rows": (t) ->
        assert.equal t.nrows(), nrows - 5

 
  "project not extend":
    topic: (t) ->
      t.project [
        'x'
        {
          alias: 'y'
          f: (x) -> x + 100
          cols: 'x'
          type: data.Schema.numeric
        }
        {
          alias: 'z'
          f: (row) -> row.get('x') * 100
          cols: '*'
          type: data.Schema.numeric
        }
        {
          alias: ['n', 'm']
          f: (x) -> {n: -x, m: -x-1000}
          type: data.Schema.numeric
          cols: 'x'
        }
      ], no

    "values correct": (t) ->
      t.each (row, idx) ->
        x = row.get 'x'
        assert.equal x, idx
        assert.equal row.get('a'), null
        assert.equal row.get('y'), (x+100), "y is wrong #{row.get 'y'} != #{x+100}"
        assert.equal row.get('z'), (x*100), "z is wrong #{row.get 'z'} != #{x*100}"
        assert.equal row.get('n'), -x, "n is wrong #{row.get 'n'} != #{-x}"
        assert.equal row.get('m'), (-x-1000), "m is wrong #{row.get 'm'} != #{-x-1000}"



  "project and extend":
    topic: (t) ->
      t.project [
        'x'
        {
          alias: 'y'
          f: (x) -> x + 100
          cols: 'x'
          type: data.Schema.numeric
        }
        {
          alias: 'z'
          f: (row) -> row.get('x') * 100
          cols: '*'
          type: data.Schema.numeric
        }
        {
          alias: ['n', 'm']
          f: (x) -> {n: -x, m: -x-1000}
          type: data.Schema.numeric
          cols: 'x'
        }
      ], yes

    "values correct": (t) ->
      t.each (row) ->
        x = row.get 'x'
        assert.equal row.get('a'), (x%2)
        assert.equal row.get('y'), (x+100), "y is wrong #{row.get 'y'} != #{x+100}"
        assert.equal row.get('z'), (x*100), "z is wrong #{row.get 'z'} != #{x*100}"
        assert.equal row.get('n'), -x, "n is wrong #{row.get 'n'} != #{-x}"
        assert.equal row.get('m'), (-x-1000), "m is wrong #{row.get 'm'} != #{-x-1000}"


  "project with unknown":
    topic: (t) ->
      t.project [
        'x'
        {
          alias: 'y'
          f: (x) -> x + 100
          cols: 'x'
          type: data.Schema.unknown
        }
        {
          alias: 'z'
          f: (row) -> data.Table.fromArray [row.raw()]
          cols: '*'
          type: data.Schema.unknown
        }
        {
          alias: 'blah'
          f: () -> 'blah'
          type: data.Schema.unknown
          cols: []
        }
      ], no

    "values correct": (t) ->
      t.each (row) ->
        x = row.get 'x'
        assert.equal row.schema.type('y'), data.Schema.numeric
        assert.equal row.schema.type('z'), data.Schema.table
        assert.equal row.schema.type('blah'), data.Schema.ordinal
        assert.equal row.get('y'), (x+100), "y is wrong #{row.get 'y'} != #{x+100}"
        row.get('z').each (subrow) ->
          assert.equal subrow.get('x'), (x), "table type z.x is wrong #{subrow.get 'x'} != #{x}"
        assert.equal row.get('blah'), "blah", "blah should be blah, instead is #{row.get 'blah'}"


  "can be turned to json and parsed back": (t) ->
    json = t.toJSON()
    t2 = data.Table.fromJSON json

    assert.deepEqual t.schema.toJSON(), t2.schema.toJSON()
    rows1 = t.all()
    rows2 = t2.all()
    _.each _.zip(rows1, rows2), ([r1, r2]) ->
      assert.equal r1.get('a'), r2.get('a'), "a's should be equal: #{r1.get('a')} != #{r2.get('a')}"


  "any() with": ->
    "no args": 
      topic: (t) -> t.any()
      "returns a row": (r) ->
        assert _.isType(r, data.Row)
    
    "string arg":
      topic: (t) -> t.any 'a'
      "returns a number": (v) ->
        assert _.isNumber(v)
    
    "array arg":
      topic: (t) -> t.any ['a', 'b']
      "returns array": (v) ->
        assert _.isArray(v)

  "all() with": ->
    "no args": 
      topic: (t) -> t.all()
      "returns array of rows": (r) ->
        assert _.isArray(r)
        for row in r
          assert _.isType(row, data.Row)
    
    "string arg":
      topic: (t) -> t.all 'a'
      "returns array of numbers": (data) ->
        assert _.isArray data
        for v in data
          assert _.isNumber(v)
    
    "array arg":
      topic: (t) -> t.all ['a', 'b']
      "returns array": (data) ->
        assert _.isArray(data)
        for coldata in data
          assert _.isArray(coldata)
          for v in coldata
            assert _.isNumber(v)




nrows = 10
rowtests = topic: makeTable nrows
_.extend rowtests, checks(nrows)

coltests = topic: -> makeTable(nrows, 'col')
_.extend coltests, checks(nrows)

partitionedtests = topic: -> new data.PartitionedTable makeTable(nrows, 'col')
_.extend partitionedtests, checks(nrows)



suite.addBatch
  "partitionedtable": partitionedtests
  "rowtable": rowtests
  "coltable": coltests
  "emptytable":
    topic: ->
      new data.RowTable new data.Schema(['x'], [data.Schema.unknown])
    
    "when unioned with itself":
      topic: (t) -> t.union t
      "works": (t) ->
        assert.equal t.nrows(), 0

    "whith empty union":
      topic: (t) -> new data.ops.Union [t, t]
      works: (t) ->
        assert.equal t.nrows(), 0





suite.export module
