require "../env"
vows = require "vows"
assert = require "assert"

try
  timer = performance
catch
  timer = Date

suite = vows.describe "perf.js"
Table = data.Table
Schema = data.Schema

makeTable = (n=10, type="row") ->
  rows = _.times n, (i) -> {
    a: i%2, 
    b: "#{i}"
    c: i%100
    d: i%5000
    x: i
    y: {
      z: i
    }
  }
  Table.fromArray rows, null, type

test = (name, n, table) ->
  perrowCosts = []
  _.times n, (i) ->
    table.each(()->)

  avgCost = table.timer().avg()
  console.log "#{name} took: #{avgCost}/all()"#\t#{avgPerRow}/outputrow\t#{d3.mean nrows} rows"



table = makeTable(1000)
niters = 20

test "base", niters, table

#union = table.union table
#test "union", niters, union
#
#filter = table.filter (row) -> row.get('x') < 100
#test "filter", niters, filter
#
lowselcoljoin = table.join table, 'c'
test "lowseljoin", niters, lowselcoljoin

highseljoin = table.join table, 'd'
test "highseljoin", niters, highseljoin
