//
//  RealmCloudantPullReplicator.swift
//  LocationTracker
//
//  Created by Mark Watson on 7/29/16.
//  Copyright © 2016 Mark Watson. All rights reserved.
//

import Foundation
import Realm
import RealmSwift

public class PullReplicator<T: Object> : Replicator {
    
    var source: CouchDBEndpoint
    var realmObjectMgr: RealmObjectManager<T>
    var replicationMgr: ReplicationManager
    var couchClient: CouchDBClient
    var completionHandler: ((ReplicationResult) -> Void)?
    
    public init(source: CouchDBEndpoint, realmObjectMgr: RealmObjectManager<T>, replicationMgr: ReplicationManager) {
        self.source = source
        self.realmObjectMgr = realmObjectMgr
        self.replicationMgr = replicationMgr
        self.couchClient = CouchDBClient(baseUrl: self.source.baseUrl, username: self.source.username, password: self.source.password)
    }
    
    public func getReplicatorId() throws -> String {
        var dict: [String: String] = [String: String]()
        dict["source"] = self.source.description
        dict["target"] = self.replicationMgr.getRealmObjectReplicatorId(realmObjectMgr: self.realmObjectMgr)
        let jsonData = try JSONSerialization.data(withJSONObject: dict, options: [])
        return CryptoUtils.sha1(input: jsonData)
    }
    
    public func start(completionHandler: @escaping (ReplicationResult) -> Void) throws {
        self.completionHandler = completionHandler
        let replicatorId = try self.getReplicatorId()
        print("REPLICATOR ID: \(replicatorId)")
        let checkpoint = self.replicationMgr.localCheckpoint(realmObjectMgr: self.realmObjectMgr, replicatorId: replicatorId)
        self.couchClient.getChanges(db: self.source.db, since: checkpoint, includeDocs: true) { (changes, error) in
            DispatchQueue.main.async {
                if error != nil {
                    self.replicationFailed(error: error, errorMessage: "Error getting changes from CouchDB")
                } else if let chgs = changes {
                    let missingDocs = self.replicationMgr.localRevsDiff(realmObjectMgr: self.realmObjectMgr, changes: chgs)
                    guard missingDocs.count > 0, let lastSequence = chgs.lastSequence else {
                        self.replicationComplete(changesProcessed: 0)
                        return
                    }
                    do {
                        let changesProcessed = try self.replicationMgr.localBulkInsert(realmObjectMgr: self.realmObjectMgr, docs: missingDocs)
                        self.replicationMgr.saveLocalCheckPoint(realmObjectMgr: self.realmObjectMgr, sequence: lastSequence)
                        self.replicationComplete(changesProcessed: changesProcessed)
                    } catch {
                        self.replicationFailed(error: error, errorMessage: "Error updating documents")
                    }
                } else {
                    self.replicationComplete(changesProcessed: 0)
                }
            }
        }
    }
    
    // MARK: Replication Complete/Cancel Functions
    
    private func replicationComplete(changesProcessed: Int) {
        self.completionHandler?(ReplicationResult(replicator: self, changesProcessed: changesProcessed))
    }
    
    private func replicationFailed(error: Error?, errorMessage: String?) {
        self.completionHandler?(ReplicationResult(replicator: self, error: error, errorMessage: errorMessage))
    }
}
