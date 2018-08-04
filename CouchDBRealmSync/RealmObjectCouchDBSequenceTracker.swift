//
//  RealmObjectCouchDBSequenceTracker.swift
//  CouchDBRealmSync
//
//  Created by Mark Watson on 8/3/16.
//  Copyright © 2016 IBM CDS Labs. All rights reserved.
//

import Foundation
import Realm
import RealmSwift

class RealmObjectCouchDBSequenceTracker {
    
    var realmObjectType: String
    var notificationToken: NotificationToken?
    
    init(realmObjectType: String) {
        self.realmObjectType = realmObjectType
    }
    
    func start(realm: Realm, completionHandler: @escaping (RealmObjectCouchDBSequence) -> Void) {
        let results = realm.objects(RealmObjectCouchDBSequence.self).filter("realmObjectType = '\(self.realmObjectType)'")
        self.notificationToken = results.observe { (changes) in
            switch changes {
            case .initial(let results):
                for obj in results {
                    completionHandler(obj)
                }
                break
            case .update(let results, _, let insertions, let modifications):
                for idx in insertions {
                    completionHandler(results[idx])
                }
                for idx in modifications {
                    completionHandler(results[idx])
                }
                break
            case .error(let err):
                fatalError("\(err)")
            }
        }
        if (results.count == 0) {
            let realmLastSeq = RealmObjectCouchDBSequence(realmObjectType: "\(realmObjectType)", lastPushSequence: Int64(0), lastPullSequence: nil)
            try! realm.write {
                realm.add(realmLastSeq)
            }
        }
    }
    
    func stop(realm: Realm) {
        if (self.notificationToken != nil) {
            self.notificationToken?.invalidate()
            self.notificationToken = nil
        }
    }
    
    func updateLastPushSequence(realm: Realm, lastPushSequence: Int64) {
        guard let results = realm.object(ofType: RealmObjectCouchDBSequence.self, forPrimaryKey: self.realmObjectType) else {
            let realmLastSeq = RealmObjectCouchDBSequence(realmObjectType: "\(self.realmObjectType)", lastPushSequence: lastPushSequence, lastPullSequence: nil)
            try! realm.write {
                realm.add(realmLastSeq)
            }
            return
        }
        try! realm.write {
            results.lastPushSequence = lastPushSequence
            realm.add(results)
        }
    }
    
    func updateLastPullSequence(realm: Realm, lastPullSequence: String) {
        guard let results = realm.object(ofType: RealmObjectCouchDBSequence.self, forPrimaryKey: self.realmObjectType) else {
            let realmLastSeq = RealmObjectCouchDBSequence(realmObjectType: "\(self.realmObjectType)", lastPushSequence: Int64(0), lastPullSequence: lastPullSequence)
            try! realm.write {
                realm.add(realmLastSeq)
            }
            return
        }
        try! realm.write {
            results.lastPullSequence = lastPullSequence
            realm.add(results)
        }
    }
}
