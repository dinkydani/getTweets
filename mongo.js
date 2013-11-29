var mongodb = require('mongodb');

var DatabaseConnection = function(mongoUri){
	mongodb.Db.connect(mongoUri, { server: { auto_reconnect: true, safe: false } }, function (err, database) {
		if(!err)
			console.log("Connected to " + mongoUri);
		if(err)
			throw err;

		this.db = database;
	});
}

//get the collection
DatabaseConnection.prototype.getCollection = function(callback) {
	db.collection('tweets', function(err, collection) {
		if( err ) callback(err);
    	else callback(null, collection);
	});
};

//find all tweets
DatabaseConnection.prototype.findAll = function(callback) {
    this.getCollection(function(err, collection) {
      if( err ) callback(err)
      else {
        collection.find().toArray(function(err, results) {
          	if( err ) {
          		console.warn(err.message);
          		callback(err);
          	}
          	else {
          		console.log("findAll successful");
          		callback(null, results);
          	}
        });
      }
    });
};

DatabaseConnection.prototype.upsert = function(data, callback){
	console.log(data.id);
	this.getCollection(function(err, collection){
		if(err) callback(err);
		else{
			collection.update({"id":data.id}, {$set: data}, {upsert:true}, function(err, object){
				if( err ) {
	          		console.warn(err.message);
	          		callback(err);
	          	}
	          	else {
	          		console.log("Upsert successful");
	          		callback(null, object);
	          	}
			});
		}
	});
};


DatabaseConnection.prototype.insert = function(data, callback){
	this.getCollection(function(err, collection){
		if(err) callback(err);
		else{
			collection.insert(data, function(){
				callback(null, data);
			});
		}
	});
};

module.exports = DatabaseConnection;