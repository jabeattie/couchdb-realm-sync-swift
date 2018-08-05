//
//  RealmObjectCouchDBSequence.swift
//  CouchDBRealmSync
//
//  Created by Mark Watson on 8/3/16.
//  Copyright © 2016 IBM CDS Labs. All rights reserved.
//

import Foundation
import Realm
import RealmSwift

/**
 One instance of this object is stored in Realm
 for each Realm object type that has been registered for replication.
 */
class RealmObjectCouchDBSequence: Object {
    
    @objc dynamic var realmObjectType: String?
    @objc dynamic var realmObjectReplicatorId: String = UUID().uuidString
    @objc dynamic var lastPushSequence: Int64 = 0
    @objc dynamic var lastPullSequence: String?
    
    override class func primaryKey() -> String? {
        return "realmObjectType"
    }
    
    convenience init(realmObjectType: String, lastPushSequence: Int64, lastPullSequence: String?) {
        self.init()
        self.realmObjectType = realmObjectType
        self.lastPushSequence = lastPushSequence
        self.lastPullSequence = lastPullSequence
    }
}
