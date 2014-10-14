var r = require('rethinkdb');
var assert = require('chai').assert;

var bedroll = require('./index.js')({});
var conn;

function filter(filter, callback){
	r.db('bedroll_test').table('test').filter(bedroll.filter(filter))('id').run(conn, function(err, cursor){
		if(err) return callback(err);
		cursor.toArray(callback);
	});
}

function sort(sort, callback){
	r.db('bedroll_test').table('test').orderBy(bedroll.sort(sort))('id').run(conn, function(err, cursor){
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

describe('Filter', function(){
	describe('Operators', function(){
		it('should support the `eq` operator', function(done){
			filter({id: {eq: '3'}}, function(err, result){
				if(err) return done(err);
				assert.lengthOf(result, 1);
				done();
			});
		});

		it('should support the `le` operator', function(done){
			filter({id: {le: '3'}}, function(err, result){
				if(err) return done(err);
				assert.lengthOf(result, 3);
				done();
			});
		});

		it('should support the `lt` operator', function(done){
			filter({id: {lt: '3'}}, function(err, result){
				if(err) return done(err);
				assert.lengthOf(result, 2);
				done();
			});
		});
	});

	describe('Traversing', function(){
		it('should support an empty filter', function(done){
			filter({}, function(err, result){
				if(err) return done(err);
				assert.lengthOf(result, 5);
				done();
			});
		});

		it('should support queries on deep properties', function(done){
			filter({atomic: {weight: {lt: '8'}}}, function(err, result){
				if(err) return done(err);
				assert.lengthOf(result, 3);
				done();
			});
		});

		it('should support queries on properties of different depths', function(done){
			filter({id: {gt: '1'}, atomic: {weight: {lt: '8'}}}, function(err, result){
				if(err) return done(err);
				assert.lengthOf(result, 2);
				done();
			});
		});
	});
});


describe('Sort', function(){
	it('should support simple sort', function(done){
		sort('name', function(err, result){
			if(err) return done(err);
			assert.equal(JSON.stringify(result), JSON.stringify([4, 5, 2, 1, 3]));
			done();
		});
	});

	it('should support descending simple sort', function(done){
		sort(['name', 'DESC'], function(err, result){
			if(err) return done(err);
			assert.equal(JSON.stringify(result), JSON.stringify([3, 1, 2, 5, 4]));
			done();
		});
	});

	it('should support simple path sort', function(done){
		sort([['name']], function(err, result){
			if(err) return done(err);
			assert.equal(JSON.stringify(result), JSON.stringify([4, 5, 2, 1, 3]));
			done();
		});
	});

	it('should support descending simple path sort', function(done){
		sort([['name'], 'DESC'], function(err, result){
			if(err) return done(err);
			assert.equal(JSON.stringify(result), JSON.stringify([3, 1, 2, 5, 4]));
			done();
		});
	});

	it('should support nested path sort', function(done){
		sort([['atomic', 'weight']], function(err, result){
			if(err) return done(err);
			assert.equal(JSON.stringify(result), JSON.stringify([1, 2, 3, 4, 5]));
			done();
		});
	});

	it('should support descending nested path sort', function(done){
		sort([['atomic', 'weight'], 'DESC'], function(err, result){
			if(err) return done(err);
			assert.equal(JSON.stringify(result), JSON.stringify([5, 4, 3, 2, 1]));
			done();
		});
	});

	it('should support multiple nested path sorts', function(done){
		sort([[['physical', 'phase'], 'ASC'], [['name'], 'ASC']], function(err, result){
			if(err) return done(err);
			assert.equal(JSON.stringify(result), JSON.stringify([2, 1, 4, 5, 3]));
			done();
		});
	});

	// This is currently failing because of https://github.com/rethinkdb/rethinkdb/issues/3188#issuecomment-58977876
	it.skip('should support multiple nested path sorts with descending', function(done){
		sort([[['physical', 'phase'], 'ASC'], [['name'], 'DESC']], function(err, result){
			if(err) return done(err);
			assert.equal(JSON.stringify(result), JSON.stringify([1, 2, 3, 5, 4]));
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