'use strict';

var r = require('rethinkdb');
var operators = {};

// build simple operators
['eq', 'ne', 'gt', 'lt', 'ge', 'le', 'match'].forEach(function(op) {
	operators[op] = function(path, val, row) {
		var cursor = row;
		path.forEach(function(segment){
			cursor = cursor(segment);
		});

		return cursor[op].call(cursor, val);
	}
});

// add the `in` operator
operators.in = function(path, val, row) {
	if(!Array.isArray(val))
		throw new Error('Array expected by `in` operator.');

	var cursor = row;
	path.forEach(function(segment){
		cursor = cursor(segment);
	});

	return r.expr(val).contains(cursor);
}


module.exports = function(config) {

	config = {
		logic: config.logic == 'or' ? 'or' : 'and'
	};

	return function filter(query, options) {
		options = options || {};
		return function(row){
			var results;

			function apply(path, val, op) {
				if(!operators[op])
					throw new Error('No such operator.');

				try {
					val = JSON.parse(val);
				} catch(e) {}

				return operators[op](path, val, row)
			}

			function traverse(scope, path) {
				Object.keys(scope).forEach(function(key) {
					if(typeof scope[key] === 'string')
						return results = results
							? results[options.logic || config.logic].call(results, apply(path, scope[key], key))
							: apply(path, scope[key], key)

					if(typeof scope[key] !== 'object')
						throw new Error('Query can only contain objects and strings.');

					var child = path.slice(); child.push(key)
					traverse(scope[key], child);
				});
			}

			traverse(query, []);

			return results;
		}
	}
}
