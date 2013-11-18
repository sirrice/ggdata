require "./env"
us = require "underscore"
assert = require "assert"

###
rows = _.times 10, (i) -> { a: i%2, x: i}
t = data.Table.fromArray rows, null, 'col'

print = (t1) ->
  console.log t1.schema.cols
  console.log t1.toString()
  console.log "\n"

print t

print(t.filter (row) -> row.get('x') < 5)

rows = _.times 10, (i) -> { x: i+ 6, b: i%3}
t2 = data.Table.fromArray rows
print t.join t2, ['x'], 'outer'

print new data.ops.Cross t, t2

print t.cross t2
###

