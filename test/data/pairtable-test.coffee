require "../env"
vows = require "vows"
assert = require "assert"



suite = vows.describe "pairtable.js"
Schema = data.Schema
Table = data.Table




createSimplePairTable = -> 
  left = Table.fromArray [
    { x: 1, y: 1, l: 1}
    { x: 1, y: 2, l: 1}
    { x: 2, y: 2, l: 3}
    { x: 2, y: 3, l: 2}
  ]
  right = Table.fromArray [
    { x: 1, l: 1, z: 0 }
    { x: 1, l: 3, z: 1 }
    { x: 2, l: 2, z: 2 }
    { x: 2, l: 1, z: 3 }
  ]

  new data.PairTable left, right

createSimpleTableSet = ->
  left = Table.fromArray [
    { x: 1, y: 1, l: 1}
    { x: 1, y: 2, l: 1}
  ]
  right = Table.fromArray [
    { x: 1, l: 1, z: 0 }
    { x: 1, l: 3, z: 1 }
  ]
  pt1 = new data.PairTable left, right

  left = Table.fromArray [
    { x: 2, y: 2, l: 3}
    { x: 2, y: 3, l: 2}
  ]
  right = Table.fromArray [
    { x: 2, l: 2, z: 2 }
    { x: 2, l: 1, z: 3 }
  ]
  pt2 = new data.PairTable left, right
  data.PairTable.union pt1, pt2

createEmptyMD = ->
  left = Table.fromArray [
    { x: 1, y: 1, l: 1}
    { x: 1, y: 2, l: 1}
    { x: 2, y: 2, l: 3}
    { x: 2, y: 3, l: 2}
  ]
  new data.PairTable left

createEmptyTable = ->
  left = Table.fromArray [
  ], new Schema(['x'], [data.Schema.numeric])
  right = Table.fromArray [
    { x: 1 }
    { x: 2 }
  ]
  new data.PairTable left, right


checkEnsure =
  "has correct shared columns": (pt) ->
    truth = _.intersection pt.left().cols(), pt.right().cols()
    assert.deepEqual pt.sharedCols(), truth

  "when ensured on x,y":
    topic: (ptable) -> ptable.ensure ['x', 'y']
    "md": 
      topic: (tset) -> tset.right()
      "has 8 rows": (md) -> assert.equal md.nrows(), 8
      "correct number of ys": (md) ->
        ps = md.partition 'y'
        for p in ps
          if p.get(0, 'y') in [1, 3]
            assert.equal p.nrows(), 2
          else 
            assert.equal p.nrows(), 4

  "when ensured on nothing":
    topic: (ptable) -> ptable.ensure []
    "md":
      topic: (tset) -> tset.right()
      "has 4 rows": (md) -> assert.equal md.nrows(), 4




suite.addBatch
  "ensure empty md":
    topic: createEmptyMD
    "when ensured on nothing":
      topic: (ptable) -> ptable.ensure []
      "md":
        topic: (tset) -> 
          tset.right()
        "has 1 row": (md) ->
          assert.equal md.nrows(), 1

  "ensure pairtable":
    _.extend({topic: createSimplePairTable},
      checkEnsure)

  "ensure tableset":
    _.extend({topic: createSimpleTableSet},
      checkEnsure)

  "empty table":
    topic: createEmptyTable
    
    "when partitioned on x":
      topic: (pairtable) ->
        pairtable.partition 'x'

      "has 2 partitions": (ps) ->
        assert.equal ps.length, 2
        for p in ps
          assert.equal p.left().nrows(), 0
          assert.equal p.right().nrows(), 1



  "pair table with dups": 
    topic: ->
      leftrows = _.times 10, (i) -> 
        { id: i % 2, a: i%5, j1: i%2, j2: i%3 }
      rightrows = _.times 10, (i) -> 
        { id: i % 2, b: "b-#{i%4}", j1: i%2, j2:i%3 }
      left = Table.fromArray leftrows
      right = Table.fromArray rightrows
      new data.PairTable left, right

    "when fully partitioned should fail": (table) ->
      assert.throws table.fullPartition



  "pair table": 
    topic: ->
      lschema = Schema.fromJSON
        id: Schema.numeric
        a: Schema.numeric
        j1: Schema.numeric
        j2: Schema.numeric
      rschema = Schema.fromJSON
        id: Schema.numeric
        b: Schema.ordinal
        j1: Schema.numeric
        j2: Schema.numeric
      leftrows = _.times 10, (i) -> 
        { id: i, a: i%5, j1: i%2, j2: i%3 }
      rightrows = _.times 10, (i) -> 
        { id: i, b: "b-#{i%4}", j1: i%2, j2:i%3 }
      left = Table.fromArray leftrows, lschema
      right = Table.fromArray rightrows, rschema

      new data.PairTable left, right

    "when partitioned on j1":
      "has 2 partitions": (table) ->
        partitions = table.partition 'j1'
        assert.equal partitions.length, 2

    "when fully partitioned":
      topic: (table) ->
        partitions = table.fullPartition()
        assert.equal partitions.length, 10


  "table set": 
    topic: ->
      lschema = Schema.fromJSON
        id: Schema.numeric
        a: Schema.numeric
        j1: Schema.numeric
        j2: Schema.numeric
      rschema = Schema.fromJSON
        id: Schema.numeric
        b: Schema.ordinal
        j1: Schema.numeric
        j2: Schema.numeric
      leftrows = _.times 10, (i) -> 
        { id: i, a: i%5, j1: i%2, j2: i%3 }
      rightrows = _.times 10, (i) -> 
        { id: i, b: "b-#{i%4}", j1: i%2, j2:i%3 }
      left = Table.fromArray leftrows, lschema
      right = Table.fromArray rightrows, rschema

      ptable1 = new data.PairTable left, right

      lschema = Schema.fromJSON
        id: Schema.numeric
        a: Schema.numeric
        j1: Schema.numeric
        j2: Schema.numeric
      rschema = Schema.fromJSON
        id: Schema.numeric
        b: Schema.ordinal
        j1: Schema.numeric
        j2: Schema.numeric
      leftrows = _.times 10, (i) -> 
        { id: i+10, a: i%5, j1: i%2, j2: i%3 }
      rightrows = _.times 10, (i) -> 
        { id: i+10, b: "b-#{i%4}", j1: i%2, j2:i%3 }
      left = Table.fromArray leftrows, lschema
      right = Table.fromArray rightrows, rschema

      ptable2 = new data.PairTable left, right
      data.PairTable.union ptable1, ptable2

    "when partitioned on j1":
      "has 2 partitions": (table) ->
        partitions = table.partition 'j1'
        assert.equal partitions.length, 2

    "when partitioned on j1, j2":
      "has 6 parts": (table) ->
        partitions = table.partition ['j1', 'j2']
        assert.equal partitions.length, 6


    "when fully partitioned":
      topic: (table) ->
        partitions = table.fullPartition()
        assert.equal partitions.length, 20



suite.export module
