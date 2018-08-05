//
//  ReplicationManager.swift
//  CouchDBRealmSync
//
//  Created by Mark Watson on 8/3/16.
//  Copyright © 2016 IBM CDS Labs. All rights reserved.
//

import Foundation
import Realm
import RealmSwift

public enum ReplicationManagerError: Error {
    case primaryKeyRequired
}

public class ReplicationManager {
    
    var realm: Realm
    var realmObjectManagers: [String: AnyObject] = [:]
    var lastSequenceTrackers: [String: RealmObjectCouchDBSequenceTracker] = [:]
    var lastSequences: [String: RealmObjectCouchDBSequence] = [:]
    
    public init(realm: Realm) {
        self.realm = realm
    }
    
    public func register<T: Object>(realmObjectType: T.Type) throws {
        guard let pk = T.primaryKey(), realmObjectType.primaryKey() != nil else {
            throw ReplicationManagerError.primaryKeyRequired
        }
        self.register(realmObjectMgr: RealmObjectManager(idField: pk, type: realmObjectType))
    }
    
    public func register<T: Object>(realmObjectMgr: RealmObjectManager<T>) {
        let realmObjectType = realmObjectMgr.getObjectType().className()
        self.realmObjectManagers[realmObjectType] = realmObjectMgr
        self.lastSequenceTrackers[realmObjectType] = RealmObjectCouchDBSequenceTracker(realmObjectType: realmObjectType)
        self.lastSequenceTrackers[realmObjectType]?.start(realm: realm, completionHandler: { (result) in
            self.lastSequences[realmObjectType] = result
        })
        realmObjectMgr.startMonitoringObjectChanges(realm: realm) { (changes) in
            self.processObjectChanges(realmObjectMgr: realmObjectMgr, changes: changes)
        }
    }
    
    public func deregister<T: Object>(realmObjectMgr: RealmObjectManager<T>) {
        realmObjectMgr.stopMonitoringObjectChanges(realm: realm)
    }
    
    public func pull<T: Object>(source: CouchDBEndpoint, target: T.Type) throws -> PullReplicator<T> {
        guard let realmObjectMgr = self.realmObjectManagers[target.className()] as? RealmObjectManager<T> else {
            throw NSError(domain: "", code: 1, userInfo: nil)
        }
        return PullReplicator(source: source, realmObjectMgr: realmObjectMgr, replicationMgr: self)
    }
    
    public func push<T: Object>(source: T.Type, target: CouchDBEndpoint) throws -> PushReplicator<T> {
        guard let realmObjectMgr = self.realmObjectManagers[source.className()] as? RealmObjectManager<T> else {
            throw NSError(domain: "", code: 1, userInfo: nil)
        }
        return PushReplicator(target: target, realmObjectMgr: realmObjectMgr, replicationMgr: self)
    }
    
    // MARK: Internal Functions
    
    func getRealmObjectReplicatorId<T: Object>(realmObjectMgr: RealmObjectManager<T>) -> String {
        return self.lastSequences[realmObjectMgr.getObjectType().className()]?.realmObjectReplicatorId ?? ""
    }
    
    func processObjectChanges<T: Object>(realmObjectMgr: RealmObjectManager<T>, changes: RealmCollectionChange<Results<T>>) {
        switch changes {
        case .initial:
            // if there are no mappings then initialize the mappings table
            if !self.atleastOneObjectMappingsExists(realmObjectMgr: realmObjectMgr) {
                self.addMissingObjectMappings(realmObjectMgr: realmObjectMgr)
            }
        case .update(let results, let deletions, let insertions, let modifications):
            self.removeObjectMappings(realmObjectMgr: realmObjectMgr, realmObjects: Array(results), indexes: deletions)
            self.addObjectMappings(realmObjectMgr: realmObjectMgr, realmObjects: Array(results), indexes: insertions)
            self.addOrUpdateObjectMappings(realmObjectMgr: realmObjectMgr, realmObjects: Array(results), indexes: modifications)
        case .error(let err):
            // An error occurred while opening the Realm file on the background worker thread
            fatalError("\(err)")
        }

    }
    
    func atleastOneObjectMappingsExists<T: Object>(realmObjectMgr: RealmObjectManager<T>) -> Bool {
        let objectType = realmObjectMgr.getObjectType().className()
        let realmDocMaps = self.realm.objects(RealmObjectCouchDBDocMap.self).filter("realmObjectType = '\(objectType)'")
        return realmDocMaps.count > 0
    }
    
    func addMissingObjectMappings<T: Object>(realmObjectMgr: RealmObjectManager<T>) {
        var realmObjectIds: [String] = []
        let objectType = realmObjectMgr.getObjectType().className()
        let realmDocMaps = self.realm.objects(RealmObjectCouchDBDocMap.self).filter("realmObjectType = '\(objectType)'")
        if realmDocMaps.count > 0 {
            for realmDocMap in realmDocMaps {
                guard let realmObjectId = realmDocMap.realmObjectId else { continue }
                realmObjectIds.append(realmObjectId)
            }
        }
        var sequence = self.lastSequences[realmObjectMgr.getObjectType().className()]?.lastPushSequence ?? Int64(0)
        let realmObjects = realmObjectMgr.getObjectsNotMatchingIds(realm: realm, ids: realmObjectIds)
        for realmObject in realmObjects {
            sequence += Int64(1)
            self.addObjectMapping(realmObjectMgr: realmObjectMgr, realmObject: realmObject, sequence: sequence)
        }
        self.lastSequenceTrackers[realmObjectMgr.getObjectType().className()]?.updateLastPushSequence(realm: realm, lastPushSequence: sequence)
    }
    
    func addObjectMappings<T: Object>(realmObjectMgr: RealmObjectManager<T>, realmObjects: [T], indexes: [Int]) {
        if indexes.count > 0 {
            var sequence = self.lastSequences[realmObjectMgr.getObjectType().className()]?.lastPushSequence ?? Int64(0)
            for idx in indexes {
                sequence += Int64(1)
                self.addObjectMapping(realmObjectMgr: realmObjectMgr, realmObject: realmObjects[idx], sequence: sequence)
            }
            self.lastSequenceTrackers[realmObjectMgr.getObjectType().className()]?.updateLastPushSequence(realm: realm, lastPushSequence: sequence)
        }
    }
    
    func addObjectMapping<T: Object>(realmObjectMgr: RealmObjectManager<T>, realmObject: T, sequence: Int64) {
        let couchDocId = UUID().uuidString
        let couchRev = "1-\(UUID().uuidString)"
        let objectType = realmObjectMgr.getObjectType().className()
        let objectId = realmObjectMgr.getObjectId(object: realmObject)
        let realmDocMap = RealmObjectCouchDBDocMap(realmObjectType: "\(objectType)",
            realmObjectId: objectId,
            couchDocId: couchDocId,
            couchRev: couchRev,
            couchSequence: sequence)
        try? self.realm.write {
            self.realm.add(realmDocMap)
        }
    }
    
    func addOrUpdateObjectMappings<T: Object>(realmObjectMgr: RealmObjectManager<T>, realmObjects: [T], indexes: [Int]) {
        if indexes.count > 0 {
            var sequence = self.lastSequences[realmObjectMgr.getObjectType().className()]?.lastPushSequence ?? Int64(0)
            for idx in indexes {
                sequence += Int64(1)
                self.addOrUpdateObjectMapping(realmObjectMgr: realmObjectMgr, realmObject: realmObjects[idx], sequence: sequence)
            }
            self.lastSequenceTrackers[realmObjectMgr.getObjectType().className()]?.updateLastPushSequence(realm: realm, lastPushSequence: sequence)
        }
    }
    
    func addOrUpdateObjectMapping<T: Object>(realmObjectMgr: RealmObjectManager<T>, realmObject: T, sequence: Int64) {
        let objectType = realmObjectMgr.getObjectType().className()
        let objectId = realmObjectMgr.getObjectId(object: realmObject)
        let realmDocMaps = self.realm.objects(RealmObjectCouchDBDocMap.self)
            .filter("realmObjectType = '\(objectType)' AND realmObjectId='\(objectId)'")
        if realmDocMaps.count > 0 {
            for realmDocMap in realmDocMaps {
                try? self.realm.write {
                    realmDocMap.couchRev = "1-\(UUID().uuidString)"
                    realmDocMap.couchSequence = sequence
                    self.realm.add(realmDocMap)
                }
            }
        } else {
            self.addObjectMapping(realmObjectMgr: realmObjectMgr, realmObject: realmObject, sequence: sequence)
        }
    }
    
    func removeObjectMappings<T: Object>(realmObjectMgr: RealmObjectManager<T>, realmObjects: [T], indexes: [Int]) {
        // TODO: increment sequence?
        //if (indexes.count > 0) {
            //for idx in indexes {
                // TODO: self.removeObjectMapping(realm, realmObjectMgr: realmObjectMgr, realmObject: realmObjects[idx])
            //}
        //}
    }
    
    func removeObjectMapping<T: Object>(realmObjectMgr: RealmObjectManager<T>, realmObject: T) {
        let objectType = realmObjectMgr.getObjectType().className()
        let objectId = realmObjectMgr.getObjectId(object: realmObject)
        let realmDocMaps = self.realm.objects(RealmObjectCouchDBDocMap.self)
            .filter("realmObjectType = '\(objectType)' AND realmObjectId='\(objectId)'")
        if realmDocMaps.count > 0 {
            for realmDocMap in realmDocMaps {
                // TODO: Need to mark that this has been deleted, so we can delete from server
                try? self.realm.write {
                    self.realm.delete(realmDocMap)
                }
            }
        }
    }
    
    func localChanges<T: Object>(realmObjectMgr: RealmObjectManager<T>, since: Int64, limit: Int32) -> RealmObjectChanges {
        var lastSequence = since
        let realmDocMaps = self.realm.objects(RealmObjectCouchDBDocMap.self)
            .filter("couchSequence > \(lastSequence) AND couchSequence <= \(lastSequence+Int64(limit))")
        if realmDocMaps.count > 0 {
            for realmDocMap in realmDocMaps {
                lastSequence = max(lastSequence, realmDocMap.couchSequence)
            }
        }
        return RealmObjectChanges(lastSequence: lastSequence, realmDocMaps: Array(realmDocMaps))
    }
    
    func localCheckpoint<T: Object>(realmObjectMgr: RealmObjectManager<T>, replicatorId: String) -> String? {
        return self.lastSequences[realmObjectMgr.getObjectType().className()]?.lastPullSequence
    }
    
    func saveLocalCheckPoint<T: Object>(realmObjectMgr: RealmObjectManager<T>, sequence: String) {
        self.lastSequenceTrackers[realmObjectMgr.getObjectType().className()]?
            .updateLastPullSequence(realm: realm, lastPullSequence: sequence)
    }
    
    func localRevsDiff<T: Object>(realmObjectMgr: RealmObjectManager<T>, changes: CouchDBChanges) -> [CouchDBBulkDoc] {
        var missingDocs = [CouchDBBulkDoc]()
        var ids = [String]()
        var revs = [String]()
        for changeRow in changes.rows {
            ids.append(changeRow.id)
            revs.append(changeRow.changes[0])
        }
        let predicate = NSPredicate(format: "couchDocId IN %@ AND couchRev IN %@", ids, revs)
        let results = self.realm.objects(RealmObjectCouchDBDocMap.self).filter(predicate)
        let matchingDocIds = results.compactMap({ $0.couchDocId })
        for changeRow in changes.rows {
            if !matchingDocIds.contains(changeRow.id) {
                let docRev = CouchDBDocRev(docId: changeRow.id, revision: changeRow.changes[0], deleted: changeRow.deleted)
                let bulkDoc = CouchDBBulkDoc(docRev: docRev, doc: changeRow.doc)
                missingDocs.append(bulkDoc)
            }
        }
        return missingDocs
    }
    
    func localBulkInsert<T: Object>(realmObjectMgr: RealmObjectManager<T>, docs: [CouchDBBulkDoc]) throws -> Int {
        var changesProcessed = 0
        // stop monitoring changes while we load
        realmObjectMgr.stopMonitoringObjectChanges(realm: self.realm)
        //
        for doc in docs {
            if doc.docRev.deleted {
                // TODO:
            } else if let internalDoc = doc.doc {
                var realmObject: T? = nil
                try self.realm.write {
                    let existingRealmObject = realmObjectMgr.getObjectById(realm: self.realm, id: doc.docRev.docId)
                    if let object = existingRealmObject {
                        realmObject = object
                        realmObjectMgr.updateObjectWithDictionary(object: object, dict: internalDoc)
                    } else {
                        realmObject = realmObjectMgr.objectFromDictionary(dict: internalDoc)
                    }
                    if let object = realmObject {
                        changesProcessed += 1
                        self.realm.add(object)
                    }
                }
                if let object = realmObject {
                    self.addOrUpdateObjectMapping(realmObjectMgr: realmObjectMgr, realmObject: object, sequence: Int64(0))
                }
            }
        }
        // start monitoring changes again
        realmObjectMgr.startMonitoringObjectChanges(realm: self.realm) { (changes) in
            self.processObjectChanges(realmObjectMgr: realmObjectMgr, changes: changes)
        }
        return changesProcessed
    }
    
}
