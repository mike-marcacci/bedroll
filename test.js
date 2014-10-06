var r = require('rethinkdb');
var assert = require('chai').assert;

var bedroll = require('./index.js')({});
var conn;

function query(filter, callback){
	r.db('bedroll_test').table('test').filter(bedroll.filter(filter)).run(conn, function(err, cursor){
		if(err) return callback(err);
		cursor.toArray(callback);
	});
}

// set up the db
before(function(done){
	r.connect({}, function(err, connection) {
		if(err) return done(err);
		conn = connection;

		// create the fixtures database
		r.dbCreate('bedroll_test').run(conn, function(err){
			if(err) return done(err);
	
			// create the fixtures table
			r.db('bedroll_test').tableCreate('test').run(conn, function(err){
				if(err) return done(err);

				// insert the fixture data
				r.db('bedroll_test').table('test').insert(require('./fixtures.json')).run(conn, done);
			});
		});
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
	it('should accept an empty filter', function(done){
		query({}, function(err, result){
			if(err) return done(err);
			assert.lengthOf(result, 5);
			done();
		});
	});

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



// drop the fixtures database
after(function(done){
	r.dbDrop('bedroll_test').run(conn, function(err){
		conn.close(done);
	});
})