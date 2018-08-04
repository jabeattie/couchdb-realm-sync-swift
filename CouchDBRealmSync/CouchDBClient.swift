//
//  CouchDBClient.swift
//  LocationTracker
//
//  Created by Mark Watson on 7/29/16.
//  Copyright © 2016 Mark Watson. All rights reserved.
//

import Foundation

public enum CouchDBError: Error {
    case EmptyResponse
}

public class CouchDBClient {
    
    var baseUrl: String
    var username: String?
    var password: String?
    
    public init(baseUrl: String) {
        self.baseUrl = baseUrl
    }
    
    public init(baseUrl: String, username: String?, password: String?) {
        self.baseUrl = baseUrl
        self.username = username
        self.password = password
    }
    
    // MARK: _all_docs
    
    public func getAllDocs(db: String, completionHandler: @escaping ([AnyObject]?, Error?) -> Void) {
        let session = URLSession.shared
        let request = self.createGetRequest(db: db, path: "_all_docs")
        let task = session.dataTask(with: request) { (data, response, err) in
            do {
                guard let dict = try self.parseResponse(data: data, response: response, error: err) else {
                    completionHandler(nil, nil)
                    return
                }
                if let rows = dict["rows"] as? [[String: AnyObject]] {
                    completionHandler(rows as [AnyObject], nil)
                } else {
                    completionHandler(nil, nil)
                }
            }
            catch {
                completionHandler(nil, error)
            }
        }
        task.resume()
    }
    
    // MARK: _bulk_docs
    
    public func bulkDocs(db: String, docs: [CouchDBBulkDoc], completionHandler: @escaping ([AnyObject]?, Error?) -> Void) {
         do {
            let bulkDocRequest = CouchDBBulkDocsReq(docs: docs)
            let body = try JSONSerialization.data(withJSONObject: bulkDocRequest.toDictionary(), options: [])
            let session = URLSession.shared
            let request = self.createPostRequest(db: db, path: "_bulk_docs", body: body)
            let task = session.dataTask(with: request) {
                (data, response, err) in
                do {
                    guard let array = try self.parseResponseAsArray(data: data, response: response, error: err) else {
                        completionHandler(nil, nil)
                        return
                    }
                    completionHandler(array, nil)
                }
                catch {
                    completionHandler(nil, error)
                }
            }
            task.resume()
        }
        catch {
            completionHandler(nil, error)
        }
    }
    
    // MARK: _changes
    
    public func getChanges(db: String, since: String?, includeDocs: Bool, completionHandler: @escaping (CouchDBChanges?, Error?) -> Void) {
        var path = "_changes?include_docs=\(includeDocs)"
        if (since != nil) {
            path = "\(path)&since=\(since!)"
        }
        let session = URLSession.shared
        let request = self.createGetRequest(db: db, path: path)
        let task = session.dataTask(with: request) {
            (data, response, err) in
            do {
                guard let dict = try self.parseResponse(data: data, response: response, error: err) else {
                    completionHandler(nil, nil)
                    return
                }
                completionHandler(CouchDBChanges(dict: dict), nil)
            }
            catch {
                completionHandler(nil, error)
            }
        }
        task.resume()
    }
    
    // MARK: _local
    
    public func saveCheckpoint(db: String, replicationId: String, lastSequence: Int64, completionHandler: @escaping (Error?) -> Void) {
        do {
            let body = try JSONSerialization.data(withJSONObject: ["lastSequence":"\(lastSequence)"], options: [])
            let session = URLSession.shared
            let request = self.createPutRequest(db: db, path: "_local/\(replicationId)", body: body)
            let task = session.dataTask(with: request) {
                (data, response, err) in
                do {
                    guard let dict = try self.parseResponse(data: data, response: response, error: err) else {
                        completionHandler(nil)
                        return
                    }
                    print("SAVE CHECKPOINT RESPONSE: \(dict)")
                    completionHandler(nil)
                } catch {
                    completionHandler(error)
                }
            }
            task.resume()
        } catch {
            completionHandler(error)
        }
    }
    
    public func getCheckpoint(db: String, replicationId: String, completionHandler: @escaping (Int64?, Error?) -> Void) {
        let session = URLSession.shared
        let request = self.createGetRequest(db: db, path: "_local/\(replicationId)")
        let task = session.dataTask(with: request) {
            (data, response, err) in
            do {
                guard let dict = try self.parseResponse(data: data, response: response, error: err) else {
                    completionHandler(nil, nil)
                    return
                }
                print("GET CHECKPOINT RESPONSE: \(dict)")
                let lastSequence: Int64? = (dict["lastSequence"] as AnyObject).longLongValue
                completionHandler(lastSequence, nil)
            } catch {
                completionHandler(nil, error)
            }
        }
        task.resume()
    }
    
    // MARK: _revs_diff
    
    public func revsDiff(db: String, docRevs: [CouchDBDocRev], completionHandler: @escaping ([CouchDBDocMissingRevs]?, Error?) -> Void) {
        do {
            var dict = [String:[String]]()
            for docRev in docRevs {
                dict[docRev.docId] = [docRev.revision]
            }
            let body = try JSONSerialization.data(withJSONObject: dict, options: [])
            let session = URLSession.shared
            let request = self.createPostRequest(db: db, path: "_revs_diff", body: body)
            let task = session.dataTask(with: request) {
                (data, response, err) in
                do {
                    guard let dict = try self.parseResponse(data: data, response: response, error: err) else {
                        completionHandler([], nil)
                        return
                    }
                    let docMissingRevs: [CouchDBDocMissingRevs] = dict.map({
                        let missingRevs = $0.value.object(forKey: "missing") as? [String] ?? []
                        return CouchDBDocMissingRevs(docId: $0.key, missingRevs: missingRevs)
                    })
                    completionHandler(docMissingRevs, nil)
                }
                catch {
                    completionHandler(nil, error)
                }
            }
            task.resume()
        }
        catch {
            completionHandler(nil, error)
        }
    }
    
    // MARK: Helper Functions
    
    func createGetRequest(db: String, path: String) -> URLRequest {
        let url = URL(string: "\(self.baseUrl)/\(db)/\(path)")
        var request = URLRequest(url: url!)
        request.addValue(" as URLapplication/json", forHTTPHeaderField:"Content-Type")
        request.addValue("application/json", forHTTPHeaderField:"Accepts")
        request.httpMethod = "GET"
        if (self.username != nil && self.password != nil) {
            let loginString = "\(self.username!):\(self.password!)"
            let loginData: Data? = loginString.data(using: .utf8)
            let base64LoginString = loginData!.base64EncodedString(options: Data.Base64EncodingOptions.lineLength64Characters)
            request.setValue("Basic \(base64LoginString)", forHTTPHeaderField: "Authorization")
        }
        return request
    }
    
    func createPostRequest(db: String, path: String, body: Data?) -> URLRequest {
        let url = URL(string: "\(self.baseUrl)/\(db)/\(path)")
        var request = URLRequest(url: url!)
        request.addValue("application/json", forHTTPHeaderField:"Content-Type")
        request.addValue("application/json", forHTTPHeaderField:"Accepts")
        request.httpMethod = "POST"
        if (body != nil) {
            request.httpBody = body
        }
        return request
    }
    
    func createPutRequest(db: String, path: String, body: Data?) -> URLRequest {
        let url = URL(string: "\(self.baseUrl)/\(db)/\(path)")
        var request = URLRequest(url: url!)
        request.addValue("application/json", forHTTPHeaderField:"Content-Type")
        request.addValue("application/json", forHTTPHeaderField:"Accepts")
        request.httpMethod = "PUT"
        if (body != nil) {
            request.httpBody = body
        }
        return request
    }
    
    func parseResponse(data: Data?, response: URLResponse?, error: Error?) throws -> [String: AnyObject]? {
        if let err = error {
            throw err
        }
        guard let data = data else {
            throw CouchDBError.EmptyResponse
        }
        return try JSONSerialization.jsonObject(with: data, options: []) as? [String: AnyObject]
    }
    
    func parseResponseAsArray(data: Data?, response: URLResponse?, error: Error?) throws -> [AnyObject]? {
        if let err = error {
            throw err
        }
        guard let data = data else {
            throw CouchDBError.EmptyResponse
        }
        return try JSONSerialization.jsonObject(with: data, options: []) as? [AnyObject]
    }
    
}
