"use strict"

# this is a problem because of how node deals with _
_ = require 'underscore'
async = require 'async'
prov = require 'ggprov'

# Need to do this because underscore is an asshole and sets the
# exports variable if we don't
exports = module.exports = @

#<< data/*

# makes sure the gg namespace has all the classes defined
_.extend @, data
@.fromArray = data.Table.fromArray.bind data.Table
@.prov = prov
