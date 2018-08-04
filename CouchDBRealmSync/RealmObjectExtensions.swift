//
//  RealmObjectExtensions.swift
//  CouchDBRealmSync
//
//  Created by Mark Watson on 8/3/16.
//  Copyright © 2016 IBM CDS Labs. All rights reserved.
//

import Foundation
import RealmSwift

extension Object {
    
    func toDictionary() -> [String:AnyObject] {
        let properties = self.objectSchema.properties.map { $0.name }
        let dictionary = self.dictionaryWithValues(forKeys: properties)
        
        let mutabledic = NSMutableDictionary()
        mutabledic.setValuesForKeys(dictionary)
        
        for prop in self.objectSchema.properties {
            // find lists
            if let nestedObject = self[prop.name] as? Object {
                mutabledic.setValue(nestedObject.toDictionary(), forKey: prop.name)
            } else if let nestedListObject = self[prop.name] as? ListBase {
                var objects = [AnyObject]()
                for index in 0..<nestedListObject._rlmArray.count {
                    let object = nestedListObject._rlmArray[index]
                    objects.append(object)
                }
                mutabledic.setValue(objects, forKey: prop.name)
            }
            
        }
        
        var dict = [String:AnyObject]()
        for key in mutabledic.allKeys {
            if let keyStr = key as? String {
                dict[keyStr] = mutabledic.object(forKey: keyStr) as AnyObject
            }
        }
        return dict
    }
    
    func updateFromDictionary(dict: [String:AnyObject]) {
        let primaryKey = type(of: self).primaryKey()
        var primaryKeyExists = false
        if (primaryKey != nil) {
            primaryKeyExists = (self.value(forKey: primaryKey!) != nil)
        }
        for prop in self.objectSchema.properties {
            if let value = dict[prop.name] {
                if let nestedObjectDict = value as? [String:AnyObject] {
                    var nestedObjectIsNew = false
                    var nestedObject = self[prop.name] as? Object
                    if (nestedObject == nil && prop.objectClassName != nil) {
                        nestedObject = Object.objectClassFromString(className: prop.objectClassName!)
                        nestedObjectIsNew = true
                    }
                    if (nestedObject != nil) {
                        nestedObject!.updateFromDictionary(dict: nestedObjectDict)
                        if (nestedObjectIsNew) {
                            self.setValue(nestedObject, forKey: prop.name)
                        }
                    }
//                } else if let nestedObjectArray = value as? [[AnyObject]] {
//                    // TODO:
//                }
                } else {
                    if (prop.name == primaryKey) {
                        if (primaryKeyExists == false) {
                            primaryKeyExists = true
                            self.setValue(value, forKey: prop.name)
                        }
                    }
                    else {
                        self.setValue(value, forKey: prop.name)
                    }
                }
            }
        }
    }
    
    class func objectClassFromString(className: String) -> Object? {
        var clazz = NSClassFromString(className) as? Object.Type
        if (clazz == nil) {
            // get the project name
            if let appName = Bundle.main.infoDictionary?["CFBundleName"] as? String {
                let classStringName = "\(appName).\(className)"
                clazz = NSClassFromString(classStringName) as? Object.Type
            }
        }
        if (clazz != nil) {
            return clazz!.init()
        } else {
            return nil
        }
    }
    
}
