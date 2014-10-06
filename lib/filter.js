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

// add the `present` operator
operators.present = function(path, val, row) {
	if(typeof val !== 'boolean')
		throw new Error('Boolean expected by `present` operator.');

	var data = {};
	var cursor = data;
	path.forEach(function(segment){
		cursor = cursor[segment] = {};
	});

	return row.hasFields(data);
}


module.exports = function(config) {
	config = config || {};
	return function filter(query, options) {
		options = options || {};
		options = {
			logic: ['or','and'].indexOf(options.logic || config.logic) > -1 ? (options.logic || config.logic) : 'and',
			silent: !!(typeof options.silent !== 'undefined' ? options.silent : typeof config.silent !== 'undefined' ? config.silent : true)
		};
		return function(row){
			var results;

			function apply(path, val, op) {
				if(!operators[op])
					throw new Error('Operator "'+op+'" is not supported.');

				try {
					val = JSON.parse(val);
				} catch(e) {}

				return operators[op](path, val, row)
			}

			function traverse(scope, path) {
				Object.keys(scope).forEach(function(key) {

					// end of the chain, let's apply the operator
					if(typeof scope[key] === 'string')
						try { return results = results
							? results[options.logic].call(results, apply(path, scope[key], key))
							: apply(path, scope[key], key)
						} catch(e){}

					// we can't process these values
					if(!scope[key] || typeof scope[key] !== 'object')
						if(options.silent)
							return;
						else
							throw new Error('Query can only contain objects and strings.');

					// traverse deeper
					var child = path.slice(); child.push(key)
					traverse(scope[key], child);
				});
			}

			if(!query || typeof query != 'object')
				if(options.silent)
					return true;
				else
					throw new Error('Filter must be an object');

			if(Object.keys(query).length === 0)
				return true;

			traverse(query, []);

			return results || true;
		}
	}
}
