const admin = require('firebase-admin')
const functions = require('firebase-functions')
const random = require('lodash')
const uuidv4 = require('uuid/v4')
const nodesPath = "nodes"
const instsPath = "instances"
const shardsPath = "shards"
const flaresPath = "flares"
const appsPath = "apps"

admin.initializeApp(functions.config().firebase)
const db = admin.firestore()
const settings = {timestampsInSnapshots: true};
db.settings(settings);

exports.registerNode = functions.https.onRequest((req, res) => {
    let doc = db.collection(nodesPath).doc(req.body.Id).set(req.body)
    doc.then(result => {
        console.log('Registered node')
        res.end()
    })
})

exports.registerInstance = functions.https.onRequest((req, res) => {
    // Get a new write batch
    let batch = db.batch()

    // Set node
    let node = req.body.node
    if (node != null) {
        let nodeRef = db.collection(nodesPath).doc(node.Id)
        batch.set(nodeRef, node)
    }

    // Set shard
    let shard = req.body.shard
    let shardRef = db.collection(shardsPath).doc(shard.Id)
    batch.set(shardRef, shard)

    // Set instance
    let instance = req.body.instance
    let instanceRef = db.collection(instsPath).doc(instance.Id)
    batch.set(instanceRef, instance)

    // Commit the batch
    batch.commit().then(function(result) {
        console.log('Registered instance')
        res.end()
    })
})

exports.countAppAndCrdbInstsInShard = functions.firestore.document(shardsPath + '/{shardId}').onUpdate((change, context) => {
    // Retrieve the current and previous value
    const data = change.after.data()
    const previousData = change.before.data()

    // We'll only update if the lengths have changed.
    // This is crucial to prevent infinite loops.
    let previousAppsLength = 0
    if (previousData.Apps != null) {
        previousAppsLength = previousData.Apps.length
    }
    let currAppsLength = 0
    if (data.Apps != null) {
        currAppsLength = data.Apps.length
    }
    if (data.CrdbInsts.length != previousData.CrdbInsts.length || currAppsLength != previousAppsLength) {
        // Then return a promise of a set operation to update the count
        change.after.ref.set({
            CrdbInstCount: data.CrdbInsts.length,
            AppsCount: currAppsLength
        }, { merge: true })
    }
})


// todo change to get under replicated or heavily loaded shards
exports.getShardToJoin = functions.https.onRequest((req, res) => {
    let shard = db.collection(shardsPath).orderBy('CrdbInstCount', 'asc').limit(1)
    shard.get().then((snapshot) => {
        // Get the last document
        if (snapshot.docs.length >= 1) {
            let last = snapshot.docs[snapshot.docs.length - 1]
            res.send(last.data())
        } else {
            console.log("No shards found")
            res.end()
        }
    })
})

exports.throwFlare = functions.https.onRequest((req, res) => {
    let doc = db.collection(flaresPath).add(req.body)
    doc.then(result => {
        console.log('Threw a flare')
        res.end()
    })
})

exports.getApp = functions.https.onRequest((req, res) => {
    let appName = req.path // expected to be in the form /app
    appName = appName.trim().replace(/^\/|\/$/g, '')
    console.log('Path: ' + req.path + ' app name: ' + appName)
    let appRef = db.collection(appsPath).doc(appName)
    appRef.get().then((doc) => {
        if (doc.exists) {
            if (doc.data().ShardJoinInfo.length >= 1) {
                // randomly pick one ip
                let randIdx = random.random(0, doc.data().ShardJoinInfo.length - 1)
                let connString = 'postgresql://root@' + doc.data().ShardJoinInfo[randIdx] + '/defaultdb'
                res.send(connString)
            }
        } else {
            res.end("No app found")
        }
    })
})

exports.createApp = functions.https.onRequest((req, res) => {
    // req.body contains just '{name : appname}'
    let appName = req.body.name

    // check if app already exists with the given name
    let appRef = db.collection(appsPath).doc(appName)
    appRef.get().then((doc) => {
        if (doc.exists) {
            res.send("AppExists: " + appName + " already exists")
        } else {
            // construct app object
            let app = {}
            app.Name = appName
            app.Id = uuidv4()

            let now = new Date().getTime();
            app.CreatedAt = now
            app.UpdatedAt = now

            // assign a shard to the app
            let shardQuery = db.collection(shardsPath).orderBy('AppsCount', 'asc').limit(1)
            let shardJoinInfo = null
            shardQuery.get().then((snapshot) => {
                // Get the last document
                if (snapshot.docs.length >= 1) {
                    let last = snapshot.docs[snapshot.docs.length - 1]
                    // Get a new write batch
                    let batch = db.batch()
                    // Add app to shard
                    let shardRef = db.collection(shardsPath).doc(last.data().Id)
                    batch.update(shardRef, { Apps: admin.firestore.FieldValue.arrayUnion(appName), UpdatedAt: now })

                    // add shard info to the app
                    app.ShardId = last.data().Id
                    app.ShardJoinInfo = last.data().JoinInfo
                    batch.set(appRef, app)

                    // Commit the batch
                    batch.commit().then(function(result) {
                        console.log('Registered app ', appName)
                        shardJoinInfo = last.data().JoinInfo
                        res.send(shardJoinInfo)
                    })

                } else {
                    console.log("No shards found to assign to app ", appName)
                    res.end()
                }
            })
        }
    })
})