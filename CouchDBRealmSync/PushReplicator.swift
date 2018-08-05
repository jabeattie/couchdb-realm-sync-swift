//
//  PushReplicator.swift
//  LocationTracker
//
//  Created by Mark Watson on 7/29/16.
//  Copyright © 2016 Mark Watson. All rights reserved.
//

import Foundation
import Realm
import RealmSwift

public class PushReplicator<T: Object> : Replicator {

    var target: CouchDBEndpoint
    var realmObjectMgr: RealmObjectManager<T>
    var replicationMgr: ReplicationManager
    var couchClient: CouchDBClient
    var completionHandler: ((ReplicationResult) -> Void)?
    
    public init(target: CouchDBEndpoint, realmObjectMgr: RealmObjectManager<T>, replicationMgr: ReplicationManager) {
        self.target = target
        self.realmObjectMgr = realmObjectMgr
        self.replicationMgr = replicationMgr
        self.couchClient = CouchDBClient(baseUrl: self.target.baseUrl, username: self.target.username, password: self.target.password)
    }
    
    public func getReplicatorId() throws -> String {
        var dict: [String: String] = [String: String]()
        dict["source"] = self.replicationMgr.getRealmObjectReplicatorId(realmObjectMgr: self.realmObjectMgr)
        dict["target"] = self.target.description
        let jsonData = try JSONSerialization.data(withJSONObject: dict, options: [])
        return CryptoUtils.sha1(input: jsonData)
    }
    
    public func start(completionHandler: @escaping (ReplicationResult) -> Void) throws {
        self.completionHandler = completionHandler
        do {
            let replicatorId = try self.getReplicatorId()
            self.couchClient.getCheckpoint(db: self.target.db, replicationId: replicatorId, completionHandler: { (lastSequence, error) in
                DispatchQueue.main.async {
                    // TODO: implement limit for real
                    let localChanges = self.getChanges(since: lastSequence, limit: Int32.max)
                    // TODO: support filter here
                    // TODO: break this up into batches?
                    let docRevs = self.getDocRevsFromChanges(changes: localChanges)
                    self.couchClient.revsDiff(db: self.target.db, docRevs: docRevs) { (missingDocRevs, error) in
                        DispatchQueue.main.async {
                            guard error == nil else {
                                self.replicationFailed(error: error, errorMessage: "Error running revsDiff")
                                return
                            }
                            guard let missingRevs = missingDocRevs, missingRevs.count > 0 else {
                                self.replicationComplete(changesProcessed: 0)
                                return
                            }
                            let docs = self.getCouchDBBulkDocs(missingDocRevs: missingRevs, changes: localChanges)
                            self.couchClient.bulkDocs(db: self.target.db, docs: docs, completionHandler: { (_, error) in
                                guard error == nil else {
                                    self.replicationFailed(error: error, errorMessage: "Error saving checkpoint")
                                    return
                                }
                                self.couchClient
                                    .saveCheckpoint(db: self.target.db,
                                                    replicationId: replicatorId,
                                                    lastSequence: localChanges.lastSequence,
                                                    completionHandler: { (error) in
                                                        guard error == nil else {
                                                            self.replicationFailed(error: error, errorMessage: "Error saving checkpoint")
                                                            return
                                                        }
                                                        self.replicationComplete(changesProcessed: missingRevs.count)
                                })
                            })
                        }
                    }
                }
            })
        } catch {
            self.replicationFailed(error: error, errorMessage: nil)
        }
    }
    
    private func getChanges(since: Int64?, limit: Int32) -> RealmObjectChanges {
        let verifiedSince: Int64 = since ?? 0
        return self.replicationMgr.localChanges(realmObjectMgr: self.realmObjectMgr, since: verifiedSince, limit: limit)
    }
    
    private func getDocRevsFromChanges(changes: RealmObjectChanges) -> [CouchDBDocRev] {
        var docRevs: [CouchDBDocRev] = []
        for realmDocMap in changes.realmDocMaps {
            guard let couchDocId = realmDocMap.couchDocId, let couchRev = realmDocMap.couchRev else { continue }
            docRevs.append(CouchDBDocRev(docId: couchDocId, revision: couchRev, deleted: false))
        }
        return docRevs
    }
    
    private func getCouchDBBulkDocs(missingDocRevs: [CouchDBDocMissingRevs], changes: RealmObjectChanges) -> [CouchDBBulkDoc] {
        var docs: [CouchDBBulkDoc] = []
        for missingDocRev in missingDocRevs {
            for missingRev in missingDocRev.missingRevs {
                for realmDocMap in changes.realmDocMaps {
                    guard let couchDocId = realmDocMap.couchDocId,
                        let couchRev = realmDocMap.couchRev,
                        let realmObjectId = realmDocMap.realmObjectId,
                        couchDocId == missingDocRev.docId,
                        couchRev == missingRev else { continue }
                    let docRev = CouchDBDocRev(docId: couchDocId, revision: couchRev, deleted: false)
                    guard let doc = self.realmObjectToDictionary(realmObjectId: realmObjectId) else { continue }
                    docs.append(CouchDBBulkDoc(docRev: docRev, doc: doc))
                }
            }
        }
        return docs
    }
    
    private func realmObjectToDictionary(realmObjectId: String) -> [String: AnyObject]? {
        let objects = self.realmObjectMgr.getObjectsMatchingIds(realm: self.replicationMgr.realm, ids: [realmObjectId])
        guard objects.count > 0 else { return nil }
        return self.realmObjectMgr.objectToDictionary(object: objects[0])
    }
    
    // MARK: Replication Complete/Cancel Functions
    
    private func replicationComplete(changesProcessed: Int) {
        self.completionHandler?(ReplicationResult(replicator: self, changesProcessed: changesProcessed))
    }
    
    private func replicationFailed(error: Error?, errorMessage: String?) {
        self.completionHandler?(ReplicationResult(replicator: self, error: error, errorMessage: errorMessage))
    }
}
