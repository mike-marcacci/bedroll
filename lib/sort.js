'use strict';

var r = require('rethinkdb');

function buildArg(path, direction) {
	var key = r.row;

	// build the key form the path
	(Array.isArray(path) ? path : [path]).forEach(function(node) {
		key = key(node);
	});

	// apply direction
	if(typeof direction === 'string' && direction.toUpperCase() === 'DESC')
		key = r.desc(key);

	return key;
}

module.exports = function(config) {
	config = config || {};
	return function sort(query, options) {
		options = options || {};
		options = {
			silent: !!(typeof options.silent !== 'undefined' ? options.silent : typeof config.silent !== 'undefined' ? config.silent : true)
		};

		// ?sort=x
		// ?sort="x"
		if(typeof query === 'string')
			return buildArg(query);

		else if(!Array.isArray(query))
			if(!options.verbose)
				return null;
			else
				throw new Error('A sort query must only be a string or an array');

		// ?sort=["x"]
		// ?sort=["x","ASC"]
		if(typeof query[0] === 'string')
			return buildArg(query[0], query[1]);

		else if(!Array.isArray(query[0]))
			if(!options.verbose)
				return null;
			else
				throw new Error('A nested sort query must only be a string or an array');

		// ?sort=[["a","b","c"]]
		// ?sort=[["a","b","c"],"DESC"]
		if(typeof query[0][0] === 'string')
			return buildArg(query[0], query[1]);

		else if(query.some(function(q){ return !Array.isArray(q[0]); }))
			if(!options.verbose)
				return null;
			else
				throw new Error('Each nested sort query must only be a string or an array');

		// ?sort[]=["x", "DESC"]&sort[]=[["a","b","c"],"ASC"]
		return r.args(query.map(function(q){
			return buildArg(q[0], q[1])
		}));
	}
}
