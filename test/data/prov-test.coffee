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

suite.addBatch
  "table":
    topic: ->
      rows = _.times 4, (i) ->  {
          a: i%2, 
          b: "#{i}"
          c: i%5
          d: i%10
          e: i%100
          f: i%500
          x: i
          y: {
            z: i
          }
      }
      t = data.fromArray rows
      t.id = 1
      t

    "project on 'a'":
      topic: (t) -> t.project 'a'

      "has correct prov": (t) ->
        t.each (row) ->
          assert.equal row.prov() , "r:1:#{row.get('x')}"

    "partition on 'a'":
      topic: (t) -> t.partition 'a'

      "each prov has 2 ids": (t) ->
        t.each (row) ->
          assert.equal row.prov().length, 2
          if row.get('a') == 0
            assert.deepEqual row.prov(), ['r:1:0', 'r:1:2']
          else
            assert.deepEqual row.prov(), ['r:1:1', 'r:1:3']


      "then aggregated": 
        topic: (t) -> t.aggregate data.ops.Aggregate.count()

        "each prov has 2 ids": (t) ->
          t.each (row) ->
            assert.equal row.prov().length, 2
            if row.get('a') == 0
              assert.deepEqual row.prov(), ['r:1:0', 'r:1:2']
            else
              assert.deepEqual row.prov(), ['r:1:1', 'r:1:3']

      "then flattened":
        topic: (t) -> t.flatten()
        "has original prov": (t) -> 
          t.each (row) ->
            assert.equal row.prov() , "r:1:#{row.get('x')}"


    "unioned with itself":
      topic: (t) -> t.union t
      "has original prov": (t) -> 
        t.each (row) ->
          assert.equal row.prov() , "r:1:#{row.get('x')}"

    "cached":
      topic: (t) -> t.cache()
      "has original prov": (t) -> 
        t.each (row) ->
          assert.equal row.prov() , "r:1:#{row.get('x')}"
    
    "once":
      topic: (t) -> t.once()
      "has original prov": (t) -> 
        t.each (row) ->
          assert.equal row.prov() , "r:1:#{row.get('x')}"

      "after running >1 times":
        topic: (t) ->
          t.all()
          t

        "has original prov": (t) -> 
          t.each (row) ->
            assert.equal row.prov() , "r:1:#{row.get('x')}"



     "with second table":
      topic: (t) ->
        r = data.fromArray [{a: 1, zz: 9}, {a:1, zz:10}, {a: 3, zz:11}]
        r.id = 2
        [t, r]

      "left join":
        topic: ([t,r]) -> t.join r, 'a', 'left'
        "provs are correct": (t) ->
          t.each (row) ->
            if row.get('a') == 0
              assert.equal row.prov().length, 1
              assert.include ['r:1:0', 'r:1:2'], row.prov()[0]
            else
              assert.equal row.prov().length, 2
              truth = [
                [ 'r:1:1', 'r:2:0' ],
                [ 'r:1:1', 'r:2:1' ],
                [ 'r:1:3', 'r:2:0' ],
                [ 'r:1:3', 'r:2:1' ]].map JSON.stringify
              assert.include truth, JSON.stringify(row.prov())

      "right join":
        topic: ([t,r]) -> t.join r, 'a', 'right'
        "provs are correct": (t) ->
          t.each (row) ->
            assert.notEqual row.get('a'), 0
            if row.get('a') == 1
              assert.equal row.prov().length, 2
              truth = [
                [ 'r:1:1', 'r:2:0' ],
                [ 'r:1:1', 'r:2:1' ],
                [ 'r:1:3', 'r:2:0' ],
                [ 'r:1:3', 'r:2:1' ]].map JSON.stringify
              assert.include truth, JSON.stringify(row.prov())
            else
              assert.equal row.prov().length, 1
              assert.include ['r:2:2'], row.prov()[0]

      "as pairtable":
        topic: ([t,r]) -> new data.PairTable t, r

        "ensured on 'a'":
          topic: (pt) -> pt.ensure 'a'
          "left is correct": (pt) ->
            l = pt.left()
            l.each (row) ->
              assert.equal row.prov() , "r:1:#{row.get('x')}"

          "right is correct": (pt) ->
            pt.right().each (row) ->
              switch row.get('a')
                when 0
                  assert.equal row.prov().length, 1
                  assert.equal row.prov()[0], 'r:1:0'
                when 1
                  assert.equal row.prov().length, 2
                  truth = [
                    [ 'r:1:1', 'r:2:0' ],
                    [ 'r:1:1', 'r:2:1' ]].map JSON.stringify
                  assert.include truth, JSON.stringify(row.prov())
                when 3
                  assert.equal row.prov().length, 1
                  assert.equal row.prov()[0], 'r:2:2'

suite.export module
