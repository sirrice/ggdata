require "../env"
vows = require "vows"
assert = require "assert"



suite = vows.describe "schema.coffee"

Schema = data.Schema

suite.addBatch
  "schema":
    topic: ->
      Schema.fromJSON
        x: Schema.numeric
        y: Schema.numeric


  "inference is correct": ->
    type = Schema.type
    assert.equal type(''), data.Schema.ordinal
    assert.equal type(10), data.Schema.numeric
    assert.equal type(1.0), data.Schema.numeric
    assert.equal type(new Date('2012/01/01')), data.Schema.date
    assert.equal type({}), data.Schema.object
    assert.equal type(data.Table.fromArray []), data.Schema.table


suite.export module
