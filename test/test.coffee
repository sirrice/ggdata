require "./env"
us = require "underscore"
assert = require "assert"


rows = _.times 1000, (i) -> { a: i%2, x: i}
t = data.fromArray rows, null, 'col'


print = (t1) ->
  console.log t1.schema.toString()
  console.log t1.raw()[0]
  console.log "timings: #{t1.timings()}"
  console.log "\n"

print t

console.log "join"
print t.join t, ['a', 'x']

console.log "cross"
cross = t.cross t
cache = cross.cache()
console.log cross.timings()
console.log cross.timings('setup')
console.log cross.timer().avg('innerloop')
print cross
console.log "cache"
console.log cache.graph()

console.log "distinct"
print t.distinct ['a']

md = data.fromArray [
  { z: 9 }
]

pt = new data.PairTable t, md
pt = pt.ensure 'a'

console.log "pt.ensure"
print pt.right()
print pt.ensure('a').right()
console.log pt.right().timings()


###

print(t.filter (row) -> row.get('x') < 5)

rows = _.times 10, (i) -> { x: i+ 6, b: i%3}
t2 = data.Table.fromArray rows
print t.join t2, ['x'], 'outer'

print new data.ops.Cross t, t2

print t.cross t2
###

