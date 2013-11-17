require "./envdata"
us = require "underscore"
assert = require "assert"
require 'datejs'

rows = _.times 10, (i) -> { a: i%2, x: i}
t = data.Table.fromArray rows, null, 'col'

rows = _.times 10, (i) -> { x: i+ 6, b: i%3}
t2 = data.Table.fromArray rows
t1 = t.join t2, ['x'], 'outer'
console.log t1.schema.cols
console.log t1.toString()
console.log "\n"


t1 = new data.ops.Cross t, t2
console.log t1.schema.cols
console.log t1.toString()
console.log "\n"

t1 = t.cross t2
console.log t1.schema.cols
console.log t1.toString()
console.log "\n"



