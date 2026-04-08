//
//  AppState.swift
//  Kit
//
//  Created by OpenAI Codex on 08/04/2026.
//

import Foundation

public class AppState {
    public static let shared = AppState()

    private let queue = DispatchQueue(label: "eu.exelban.Stats.AppState")
    private var modules: [String: [String: Any]] = [:]
    private var publishWorkItem: DispatchWorkItem?

    public init() {}

    public func update<T: Codable>(moduleKey: String, value: T) {
        guard let data = try? JSONEncoder().encode(value),
              let json = try? JSONSerialization.jsonObject(with: data, options: [.fragmentsAllowed]) else { return }

        self.queue.async {
            let parts = moduleKey.split(separator: "@", maxSplits: 1, omittingEmptySubsequences: false)
            guard let module = parts.first.map(String.init), !module.isEmpty else { return }
            let reader = parts.indices.contains(1) ? String(parts[1]) : "value"

            var moduleState = self.modules[module] ?? [:]
            moduleState[reader] = json
            self.modules[module] = moduleState

            self.schedulePublishLocked()
        }
    }

    private func schedulePublishLocked() {
        self.publishWorkItem?.cancel()

        let work = DispatchWorkItem { [weak self] in
            guard let self, let payload = self.snapshotLocked() else { return }
            LocalSocketExporter.shared.publish(payload)
        }

        self.publishWorkItem = work
        self.queue.asyncAfter(deadline: .now() + 0.05, execute: work)
    }

    private func snapshotLocked() -> Data? {
        let state: [String: Any] = [
            "ts": Date().timeIntervalSince1970,
            "modules": self.modules
        ]

        return try? JSONSerialization.data(withJSONObject: state, options: [.sortedKeys])
    }
}
