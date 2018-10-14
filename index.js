const admin = require('firebase-admin')
const functions = require('firebase-functions')
const uuidv4 = require('uuid/v4');
const nodesPath = "nodes"
const instsPath = "instances"
const shardsPath = "shards"
const flaresPath = "flares"
const appsPath = "apps"

admin.initializeApp(functions.config().firebase)
const db = admin.firestore()

exports.helloGET = (req, res) => {
    res.send('Hello World!')
};

exports.registerNode = (req, res) => {
    var doc = db.collection(nodesPath).doc(req.body.Id).set(req.body)
    doc.then(result => {
        console.log('Registered node')
        res.end()
    })
}

exports.registerInstance = (req, res) => {
    // Get a new write batch
    var batch = db.batch()

    // Set node
    var node = req.body.node
    if (node != null) {
        var nodeRef = db.collection(nodesPath).doc(node.Id)
        batch.set(nodeRef, node)
    }

    // Set shard
    var shard = req.body.shard
    var shardRef = db.collection(shardsPath).doc(shard.Id)
    batch.set(shardRef, shard)

    // Set instance
    var instance = req.body.instance
    var instanceRef = db.collection(instsPath).doc(instance.Id)
    batch.set(instanceRef, instance)

    // Commit the batch
    batch.commit().then(function(result) {
        console.log('Registered instance')
        res.end()
    });
}

// todo change to get under replicated or heavily loaded shards
exports.getShardToJoin = (req, res) => {
    var shard = db.collection(shardsPath).orderBy('UpdatedAt', 'asc').limit(1)
    shard.get().then((snapshot) => {
        // Get the last document
        if (snapshot.docs.length >= 1) {
            var last = snapshot.docs[snapshot.docs.length - 1]
            res.send(last.data())
        } else {
            console.log("No shards found")
            res.end()
        }
    })
}

exports.throwFlare = (req, res) => {
    var doc = db.collection(flaresPath).add(req.body)
    doc.then(result => {
        console.log('Threw a flare')
        res.end()
    })
}

exports.registerApp = (req, res) => {
    // req.body contains just '{name : appname}'
    var appName = req.body.name

    // check if app already exists with the given name
    var appRef = db.collection(appsPath).doc(appName)
    appRef.get().then((doc) => {
        if (doc.exists) {
            res.send("AppExists: " + appName + " already exists")
        } else {
            // construct app object
            var app = {}
            app.name = appName
            app.id = uuidv4()
            var now = new Date()
            app.createdAt = now
            app.updatedAt = now

            // assign a shard to the app
            var shardQuery = db.collection(shardsPath).orderBy('UpdatedAt', 'asc').limit(1)
            var shardJoinInfo = null
            shardQuery.get().then((snapshot) => {
                // Get the last document
                if (snapshot.docs.length >= 1) {
                    var last = snapshot.docs[snapshot.docs.length - 1]
                    // Get a new write batch
                    var batch = db.batch()
                    // Add app to shard
                    var shardRef = db.collection(shardsPath).doc(last.data().Id)
                    batch.update(shardRef, { apps: admin.firestore.FieldValue.arrayUnion(appName), UpdatedAt: now })

                    // add shard info to the app
                    app.shardId = last.data().Id
                    batch.set(appRef, app)

                    // Commit the batch
                    batch.commit().then(function(result) {
                        console.log('Registered app ', appName)
                        shardJoinInfo = last.data().JoinInfo
                        res.send(shardJoinInfo)
                    });

                } else {
                    console.log("No shards found to assign to app ", appName)
                    res.end()
                }
            })
        }
    })
}