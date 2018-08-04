//
//  RealmCloudantPullReplicator.swift
//  LocationTracker
//
//  Created by Mark Watson on 7/29/16.
//  Copyright © 2016 Mark Watson. All rights reserved.
//

import CryptoSwift
import Foundation

public class CryptoUtils {
    
    static let SHA1_DIGEST_LENGTH = 20
    
    public static func sha1(input: Data) -> String {
        let hash = input.bytes.sha1()
        return hexStringFromData(input: Data(bytes: hash, count: SHA1_DIGEST_LENGTH))
    }
    
    private static func hexStringFromData(input: Data) -> String {
        var hexString = ""
        for byte in input.bytes {
            hexString += String(format:"%02x", UInt8(byte))
        }
        return hexString
    }
}
