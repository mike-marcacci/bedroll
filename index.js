module.exports = function(config){
	return {
		filter: require('./lib/filter.js')(config),
		sort: require('./lib/sort.js')(config)
	}
};