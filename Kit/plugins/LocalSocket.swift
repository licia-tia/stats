//
//  LocalSocket.swift
//  Kit
//
//  Created by OpenAI Codex on 08/04/2026.
//

import Cocoa
import Darwin

public class LocalSocketExporter {
    public static let shared = LocalSocketExporter()

    public var isEnabled: Bool {
        get { Store.shared.bool(key: "localSocketExport", defaultValue: false) }
        set {
            Store.shared.set(key: "localSocketExport", value: newValue)
            if newValue {
                self.startIfNeeded()
            } else {
                self.stop()
            }
        }
    }

    public var socketURL: URL {
        let supportPath = FileManager.default.urls(for: .applicationSupportDirectory, in: .userDomainMask).first!
            .appendingPathComponent("Stats", isDirectory: true)
        return supportPath.appendingPathComponent("stats.sock", isDirectory: false)
    }

    public var socketPath: String { self.socketURL.path }

    private let log: NextLog
    private let queue = DispatchQueue(label: "eu.exelban.Stats.LocalSocket")

    private var listenerFD: Int32 = -1
    private var listenerSource: DispatchSourceRead?
    private var clients: [Int32] = []
    private var latestPayload: Data?

    public init() {
        self.log = NextLog.shared.copy(category: "Local socket")
    }

    public func startIfNeeded() {
        self.queue.async {
            guard self.isEnabled, self.listenerFD == -1 else { return }
            self.startLocked()
        }
    }

    public func publish(_ payload: Data) {
        self.queue.async {
            self.latestPayload = payload
            guard self.isEnabled else { return }
            if self.listenerFD == -1 {
                self.startLocked()
            }
            self.broadcastLocked(payload)
        }
    }

    public func stop() {
        self.queue.async {
            self.stopLocked()
        }
    }

    public func terminate() {
        self.stop()
    }

    private func startLocked() {
        let fileManager = FileManager.default
        let directory = self.socketURL.deletingLastPathComponent()

        do {
            try fileManager.createDirectory(at: directory, withIntermediateDirectories: true)
            try fileManager.setAttributes([.posixPermissions: 0o700], ofItemAtPath: directory.path)
        } catch let err {
            error("failed to prepare socket directory: \(err.localizedDescription)", log: self.log)
            return
        }

        unlink(self.socketPath)

        let fd = socket(AF_UNIX, SOCK_STREAM, 0)
        guard fd >= 0 else {
            error("failed to create socket: \(String(cString: strerror(errno)))", log: self.log)
            return
        }

        var yes: Int32 = 1
        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, socklen_t(MemoryLayout<Int32>.size))

        let flags = fcntl(fd, F_GETFL, 0)
        if flags >= 0 {
            _ = fcntl(fd, F_SETFL, flags | O_NONBLOCK)
        }

        var address = sockaddr_un()
        address.sun_family = sa_family_t(AF_UNIX)

        let maxLength = MemoryLayout.size(ofValue: address.sun_path)
        let utf8Path = self.socketPath.utf8CString
        guard utf8Path.count <= maxLength else {
            close(fd)
            error("socket path is too long: \(self.socketPath)", log: self.log)
            return
        }

        withUnsafeMutablePointer(to: &address.sun_path) { pointer in
            pointer.withMemoryRebound(to: CChar.self, capacity: maxLength) { buffer in
                buffer.initialize(repeating: 0, count: maxLength)
                _ = strncpy(buffer, self.socketPath, maxLength - 1)
            }
        }

        let bindResult = withUnsafePointer(to: &address) {
            $0.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                Darwin.bind(fd, $0, socklen_t(MemoryLayout<sockaddr_un>.size))
            }
        }

        guard bindResult == 0 else {
            error("failed to bind socket: \(String(cString: strerror(errno)))", log: self.log)
            close(fd)
            unlink(self.socketPath)
            return
        }

        chmod(self.socketPath, mode_t(0o600))

        guard listen(fd, SOMAXCONN) == 0 else {
            error("failed to listen on socket: \(String(cString: strerror(errno)))", log: self.log)
            close(fd)
            unlink(self.socketPath)
            return
        }

        self.listenerFD = fd

        let source = DispatchSource.makeReadSource(fileDescriptor: fd, queue: self.queue)
        source.setEventHandler { [weak self] in
            self?.acceptClientsLocked()
        }
        source.setCancelHandler { [weak self] in
            guard let self else { return }
            if self.listenerFD >= 0 {
                close(self.listenerFD)
                self.listenerFD = -1
            }
        }
        source.resume()
        self.listenerSource = source

        debug("local socket export started at \(self.socketPath)", log: self.log)
    }

    private func stopLocked() {
        self.listenerSource?.cancel()
        self.listenerSource = nil

        self.clients.forEach { fd in
            close(fd)
        }
        self.clients.removeAll()

        if self.listenerFD >= 0 {
            close(self.listenerFD)
            self.listenerFD = -1
        }

        unlink(self.socketPath)
        debug("local socket export stopped", log: self.log)
    }

    private func acceptClientsLocked() {
        while true {
            let client = accept(self.listenerFD, nil, nil)
            if client < 0 {
                if errno == EAGAIN || errno == EWOULDBLOCK {
                    break
                }
                error("failed to accept socket client: \(String(cString: strerror(errno)))", log: self.log)
                break
            }

            var yes: Int32 = 1
            setsockopt(client, SOL_SOCKET, SO_NOSIGPIPE, &yes, socklen_t(MemoryLayout<Int32>.size))

            let flags = fcntl(client, F_GETFL, 0)
            if flags >= 0 {
                _ = fcntl(client, F_SETFL, flags | O_NONBLOCK)
            }

            self.clients.append(client)
            debug("local socket client connected", log: self.log)

            if let latestPayload = self.latestPayload, !self.sendLocked(latestPayload, to: client) {
                self.removeClientLocked(client)
            }
        }
    }

    private func broadcastLocked(_ payload: Data) {
        guard !self.clients.isEmpty else { return }

        var alive: [Int32] = []
        for client in self.clients {
            if self.sendLocked(payload, to: client) {
                alive.append(client)
            } else {
                close(client)
            }
        }
        self.clients = alive
    }

    private func sendLocked(_ payload: Data, to client: Int32) -> Bool {
        var message = payload
        message.append(0x0A)

        return message.withUnsafeBytes { buffer in
            guard let base = buffer.baseAddress else { return false }

            var sent = 0
            while sent < buffer.count {
                let next = base.advanced(by: sent)
                let result = Darwin.send(client, next, buffer.count - sent, 0)

                if result > 0 {
                    sent += result
                    continue
                }

                if result == -1 && (errno == EAGAIN || errno == EWOULDBLOCK) {
                    break
                }

                debug("local socket client disconnected", log: self.log)
                return false
            }

            return true
        }
    }

    private func removeClientLocked(_ client: Int32) {
        if let index = self.clients.firstIndex(of: client) {
            self.clients.remove(at: index)
        }
        close(client)
    }
}
