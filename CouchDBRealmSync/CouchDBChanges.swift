//
//  CouchDBChanges.swift
//  CouchDBRealmSync
//
//  Created by Mark Watson on 8/4/16.
//  Copyright © 2016 IBM CDS Labs. All rights reserved.
//

import Foundation

public class CouchDBChanges {
    
    var lastSequence: String?
    var pending: Int?
    var rows: [CouchDBChangeRow]
    
    public init(dict: [String: AnyObject]) {
        self.lastSequence = dict["last_seq"] as? String
        self.pending = dict["pending"] as? Int
        self.rows = [CouchDBChangeRow]()
        guard let resultsArray = dict["results"] as? [[String: AnyObject]] else { return }
        for resultDict in resultsArray {
            self.rows.append(CouchDBChangeRow(dict: resultDict))
        }
    }

}
