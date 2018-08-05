//
//  CouchDBChangeRow.swift
//  CouchDBRealmSync
//
//  Created by Mark Watson on 8/4/16.
//  Copyright © 2016 IBM CDS Labs. All rights reserved.
//

import Foundation

public class CouchDBChangeRow {
    
    var seq: String
    var id: String
    var changes: [String]
    var deleted: Bool
    var doc: [String: AnyObject]?
    
    public init(dict: [String: AnyObject]) {
        self.seq = dict["seq"] as? String ?? ""
        self.id = dict["id"] as? String ?? ""
        self.deleted = dict["deleted"] as? Bool ?? false
        self.doc = dict["doc"] as? [String: AnyObject]
        self.changes = [String]()
        guard let changesArray = dict["changes"] as? [[String: AnyObject]] else { return }
        for changesDict in changesArray {
            guard let change = changesDict["rev"] as? String else { continue }
            self.changes.append(change)
        }
    }
    
}
