var r = require('rethinkdb');
var assert = require('chai').assert;

var bedroll = require('./index.js')({});
var conn;

function query(filter, callback){
	r.db('test').table('test').filter(bedroll.filter(filter)).run(conn, function(err, cursor){
		if(err) return callback(err);
		cursor.toArray(callback);
	});
}

before(function(done){
	// connect to the db
	r.connect({db: 'test'}, function(err, connection) {
		if(err) return done(err);
		conn = connection;

		// TODO: create the fixtures database

		// TODO: create the fixtures table

		// TODO: insert the fixtures

		done();
	});
});

describe('Operators', function(done){
	it('should support the `eq` operator', function(done){
		query({id: {eq: '3'}}, function(err, result){
			if(err) return done(err);
			assert.lengthOf(result, 1);
			done();
		});
	});

	it('should support the `le` operator', function(done){
		query({id: {le: '3'}}, function(err, result){
			if(err) return done(err);
			assert.lengthOf(result, 3);
			done();
		});
	});

	it('should support the `lt` operator', function(done){
		query({id: {lt: '3'}}, function(err, result){
			if(err) return done(err);
			assert.lengthOf(result, 2);
			done();
		});
	});
});

describe('Traversing', function(done){
	it('should support queries on deep properties', function(done){
		query({atomic: {weight: {lt: '8'}}}, function(err, result){
			if(err) return done(err);
			assert.lengthOf(result, 3);
			done();
		});
	});

	it('should support queries on properties of different depths', function(done){
		query({id: {gt: '1'}, atomic: {weight: {lt: '8'}}}, function(err, result){
			if(err) return done(err);
			assert.lengthOf(result, 2);
			done();
		});
	});
});




after(function(done){

	// TODO: drop the fixtures database

	conn.close(done);
})