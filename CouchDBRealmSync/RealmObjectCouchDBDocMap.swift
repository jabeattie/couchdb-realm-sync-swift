//
//  RealmObjectCouchDBDocMap.swift
//  CouchDBRealmSync
//
//  Created by Mark Watson on 8/2/16.
//  Copyright © 2016 IBM CDS Labs. All rights reserved.
//

import Realm
import RealmSwift

/**
 One instance of this class is stored in Realm
 for each Realm object that is being tracked for replication.
 */
class RealmObjectCouchDBDocMap : Object {
    
    @objc dynamic var realmObjectType: String?
    @objc dynamic var realmObjectId: String?
    @objc dynamic var couchDocId: String?
    @objc dynamic var couchRev: String?
    @objc dynamic var couchSequence: Int64 = 0
    
    convenience init(realmObjectType: String, realmObjectId: String, couchDocId: String, couchRev: String, couchSequence: Int64) {
        self.init()
        self.realmObjectType = realmObjectType;
        self.realmObjectId = realmObjectId
        self.couchDocId = couchDocId
        self.couchRev = couchRev
        self.couchSequence = couchSequence
    }
    
}
