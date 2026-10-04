#!/usr/bin/env python3
"""
Complete Production Chain Context System
Integrates production chain calculation with context state management
Enhanced with SFSO queue, database persistence, and PI irrational rectification.
"""

import json
import math
import os
import sqlite3
from collections import defaultdict, deque
from datetime import datetime, timezone
from threading import Lock
from typing import Any, Dict, List, Optional, Tuple

try:
    from production_chain_calculator import (
        ProductionChainCalculator,
        ProductionUnit,
        ProductionSystemType,
        ChainProduction,
    )
except Exception:  # pragma: no cover - fallback for standalone execution
    class ProductionSystemType:
        EHR_SYSTEM = "EHR_SYSTEM"
        MONITORING_SYSTEM = "MONITORING_SYSTEM"
        DIAGNOSTIC_SYSTEM = "DIAGNOSTIC_SYSTEM"
        MODEL_INFERENCE = "MODEL_INFERENCE"
        MODEL_CALL = "MODEL_CALL"
        PERSONAL_RECOGNITION = "PERSONAL_RECOGNITION"

        @classmethod
        def __getitem__(cls, key: str):
            return getattr(cls, key)

    class ProductionUnit:
        def __init__(self, unit_id: str, system_type: str):
            self.unit_id = unit_id
            self.system_type = system_type

    class ChainProduction:
        def __init__(self, chain_id: str, user_id: str):
            self.chain_id = chain_id
            self.user_id = user_id
            self.model_responses: List[Dict[str, Any]] = []
            self.production_units: List[ProductionUnit] = []

        def record_model_response(
            self,
            response_text: str,
            response_data: Dict[str, Any],
            model_id: str = "default",
            confidence: float = 0.85,
        ) -> Dict[str, Any]:
            payload = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "model_id": model_id,
                "response_text": response_text,
                "response_data": response_data,
                "confidence": confidence,
                "text_to_class_terminologies": [
                    f"{k}:{v}" for k, v in (response_data or {}).items()
                ],
            }
            self.model_responses.append(payload)
            return payload

    class ProductionChainCalculator:
        def __init__(self):
            self.system_registry: Dict[str, ProductionUnit] = {}
            self.chains: Dict[str, ChainProduction] = {}

        def create_production_chain(self, chain_id: str, user_id: str) -> ChainProduction:
            chain = ChainProduction(chain_id, user_id)
            self.chains[chain_id] = chain
            return chain

        def register_production_system(self, unit: ProductionUnit) -> None:
            self.system_registry[unit.unit_id] = unit

        def calculate_chain_from_production(
            self,
            chain_id: str,
            production_unit_ids: List[str],
            thresholds: Optional[Dict[str, float]] = None,
        ) -> Dict[str, Any]:
            thresholds = thresholds or {}
            return {
                "chain_id": chain_id,
                "production_unit_ids": production_unit_ids,
                "thresholds": thresholds,
                "status": "ok",
                "units_registered": len(production_unit_ids),
            }

        def export_permanent_buffer(self, path: str) -> None:
            with open(path, "w", encoding="utf-8") as handle:
                json.dump({"chains": list(self.chains.keys())}, handle, indent=2)


class DatabaseManager:
    """Manages SQLite database for persistent state storage."""

    def __init__(self, db_path: str = "production_context.db"):
        self.db_path = db_path
        self.lock = Lock()
        self._initialize_db()

    def _initialize_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    snapshot_key TEXT UNIQUE NOT NULL,
                    chain_id TEXT NOT NULL,
                    context_state TEXT NOT NULL,
                    prod_selection TEXT,
                    chain_movement TEXT,
                    saved_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS server_states (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chain_id TEXT NOT NULL,
                    server_id TEXT NOT NULL,
                    state_data TEXT NOT NULL,
                    dependencies TEXT,
                    served_order INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(chain_id, server_id)
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS pi_calculations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chain_id TEXT NOT NULL,
                    calculation_type TEXT NOT NULL,
                    pi_value REAL NOT NULL,
                    commodity_trial TEXT,
                    irrational_rectification REAL,
                    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS access_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    action TEXT NOT NULL,
                    chain_id TEXT,
                    details TEXT
                )
                """
            )
            conn.commit()

    def save_snapshot(
        self,
        snapshot_key: str,
        chain_id: str,
        context_state: Dict[str, Any],
        prod_selection: str,
        chain_movement: str,
    ) -> bool:
        with self.lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO snapshots
                        (snapshot_key, chain_id, context_state, prod_selection, chain_movement)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            snapshot_key,
                            chain_id,
                            json.dumps(context_state),
                            prod_selection,
                            chain_movement,
                        ),
                    )
                    conn.commit()
                return True
            except Exception as exc:  # pragma: no cover
                print(f"Error saving snapshot: {exc}")
                return False

    def retrieve_snapshot(self, snapshot_key: str) -> Optional[Dict[str, Any]]:
        with self.lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    row = conn.execute(
                        "SELECT context_state FROM snapshots WHERE snapshot_key = ?",
                        (snapshot_key,),
                    ).fetchone()
                    if not row:
                        return None
                    return json.loads(row[0])
            except Exception as exc:  # pragma: no cover
                print(f"Error retrieving snapshot: {exc}")
                return None

    def save_server_state(
        self,
        chain_id: str,
        server_id: str,
        state_data: Dict[str, Any],
        dependencies: List[str],
        served_order: int,
    ) -> bool:
        with self.lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute(
                        """
                        INSERT OR REPLACE INTO server_states
                        (chain_id, server_id, state_data, dependencies, served_order)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            chain_id,
                            server_id,
                            json.dumps(state_data),
                            json.dumps(dependencies),
                            served_order,
                        ),
                    )
                    conn.commit()
                return True
            except Exception as exc:  # pragma: no cover
                print(f"Error saving server state: {exc}")
                return False

    def get_server_state(self, chain_id: str, server_id: str) -> Optional[Dict[str, Any]]:
        with self.lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    row = conn.execute(
                        """
                        SELECT state_data, dependencies, served_order
                        FROM server_states
                        WHERE chain_id = ? AND server_id = ?
                        """,
                        (chain_id, server_id),
                    ).fetchone()
                    if row:
                        return {
                            "state_data": json.loads(row[0]),
                            "dependencies": json.loads(row[1]),
                            "served_order": row[2],
                        }
                    return None
            except Exception as exc:  # pragma: no cover
                print(f"Error getting server state: {exc}")
                return None

    def save_pi_calculation(
        self,
        chain_id: str,
        calculation_type: str,
        pi_value: float,
        commodity_trial: str,
        irrational_rectification: float,
    ) -> bool:
        with self.lock:
            try:
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute(
                        """
                        INSERT INTO pi_calculations
                        (chain_id, calculation_type, pi_value, commodity_trial, irrational_rectification)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (
                            chain_id,
                            calculation_type,
                            pi_value,
                            commodity_trial,
                            irrational_rectification,
                        ),
                    )
                    conn.commit()
                return True
            except Exception as exc:  # pragma: no cover
                print(f"Error saving PI calculation: {exc}")
                return False


class SFSOQueueManager:
    """Manages a served-first-served-out queue."""

    def __init__(self):
        self.queue: Dict[str, deque] = defaultdict(deque)
        self.served_order: Dict[str, int] = defaultdict(int)
        self.lock = Lock()

    def enqueue(self, chain_id: str, server_id: str, state_data: Dict[str, Any]) -> int:
        with self.lock:
            order = self.served_order[chain_id]
            self.served_order[chain_id] += 1
            self.queue[chain_id].append(
                {
                    "server_id": server_id,
                    "state_data": state_data,
                    "served_order": order,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            )
            return order

    def dequeue(self, chain_id: str) -> Optional[Dict[str, Any]]:
        with self.lock:
            if chain_id in self.queue and self.queue[chain_id]:
                return self.queue[chain_id].popleft()
            return None

    def get_queue_status(self, chain_id: str) -> Dict[str, Any]:
        with self.lock:
            queue = self.queue.get(chain_id, deque())
            return {
                "queue_length": len(queue),
                "served_count": self.served_order.get(chain_id, 0),
                "current_order": self.served_order.get(chain_id, 0),
            }


class PIRationalRectificationCalculator:
    """Calculates PI with irrational rectification based on commodity trials."""

    @staticmethod
    def calculate_irrational_rectification(
        base_value: float,
        commodity_trial_factor: float,
    ) -> float:
        pi_base = math.pi
        phi = (1 + math.sqrt(5)) / 2
        e_adjustment = math.e
        numerator = pi_base * commodity_trial_factor * math.log(phi + commodity_trial_factor)
        denominator = math.sqrt(e_adjustment)
        return numerator / denominator + (base_value * 0.01)

    @staticmethod
    def rectify_pi_value(
        raw_pi: float,
        commodity_trial: str,
        trial_intensity: float = 1.0,
    ) -> Tuple[float, float]:
        trial_factors = {
            "high": 1.8,
            "medium": 1.2,
            "low": 0.8,
            "critical": 2.5,
            "low-medium": 1.8,
            "high-fast": 2.2,
            "medium-low": 0.8,
        }
        normalized = str(commodity_trial or "medium").lower()
        factor = trial_factors.get(normalized, 1.0)
        effective_factor = factor * trial_intensity
        irrational_rect = PIRationalRectificationCalculator.calculate_irrational_rectification(
            raw_pi,
            effective_factor,
        )
        rectified_pi = raw_pi + irrational_rect
        return rectified_pi, irrational_rect


class PermanentContextBuffer:
    """Manages permanent storage and retrieval of context states."""

    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.buffer_store: Dict[str, Dict[str, Any]] = {}
        self.access_log: List[Dict[str, Any]] = []
        self.snapshots: Dict[str, List[str]] = {}

    def save_snapshot(
        self,
        chain_id: str,
        context_state: Dict[str, Any],
        prod_selection: str = "all",
        chain_movement: str = "forward",
    ) -> str:
        snapshot_key = f"{chain_id}_{datetime.now(timezone.utc).timestamp()}"
        snapshot = {
            "snapshot_key": snapshot_key,
            "chain_id": chain_id,
            "saved_at": datetime.now(timezone.utc).isoformat(),
            "prod_selection": prod_selection,
            "chain_movement": chain_movement,
            "context_state": context_state,
        }

        self.buffer_store[snapshot_key] = snapshot
        self.db.save_snapshot(snapshot_key, chain_id, context_state, prod_selection, chain_movement)

        if chain_id not in self.snapshots:
            self.snapshots[chain_id] = []
        self.snapshots[chain_id].append(snapshot_key)

        self.access_log.append(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "action": "save",
                "snapshot_key": snapshot_key,
                "chain_id": chain_id,
            }
        )
        return snapshot_key

    def retrieve_snapshot(self, snapshot_key: str) -> Optional[Dict[str, Any]]:
        if snapshot_key in self.buffer_store:
            snapshot = self.buffer_store[snapshot_key]
        else:
            snapshot = self.db.retrieve_snapshot(snapshot_key)
            if snapshot:
                self.buffer_store[snapshot_key] = snapshot

        if snapshot:
            self.access_log.append(
                {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "action": "retrieve",
                    "snapshot_key": snapshot_key,
                }
            )
        return snapshot

    def get_chain_snapshots(self, chain_id: str) -> List[Dict[str, Any]]:
        if chain_id not in self.snapshots:
            return []
        return [self.buffer_store[key] for key in self.snapshots[chain_id] if key in self.buffer_store]

    def reinstate_with_prod_selection(
        self,
        chain_id: str,
        prod_selection: str,
        restore_point: Optional[str] = None,
    ) -> Dict[str, Any]:
        snapshots = self.get_chain_snapshots(chain_id)
        if not snapshots:
            return {"error": f"No snapshots for chain {chain_id}"}

        target_snapshot = None
        if restore_point:
            target_snapshot = next((s for s in snapshots if s["snapshot_key"] == restore_point), None)
        if target_snapshot is None:
            target_snapshot = snapshots[-1]

        filtered_state = self._filter_by_prod_selection(
            target_snapshot["context_state"],
            prod_selection,
        )
        return {
            "restored_at": datetime.now(timezone.utc).isoformat(),
            "from_snapshot": target_snapshot["snapshot_key"],
            "original_save_time": target_snapshot["saved_at"],
            "prod_selection_applied": prod_selection,
            "context_state": filtered_state,
        }

    def _filter_by_prod_selection(
        self,
        context_state: Dict[str, Any],
        prod_selection: str,
    ) -> Dict[str, Any]:
        if prod_selection == "all":
            return context_state

        selected_systems = set(part.strip() for part in prod_selection.split(",") if part.strip())
        filtered = dict(context_state)

        if "production_systems_involved" in filtered:
            filtered["production_systems_involved"] = [
                system
                for system in filtered["production_systems_involved"]
                if system in selected_systems
            ]
        return filtered

    def export_buffer(self, output_file: str) -> None:
        with open(output_file, "w", encoding="utf-8") as handle:
            json.dump(
                {
                    "exported_at": datetime.now(timezone.utc).isoformat(),
                    "total_snapshots": len(self.buffer_store),
                    "chains": len(self.snapshots),
                    "snapshots": self.buffer_store,
                    "access_log_entries": len(self.access_log),
                },
                handle,
                indent=2,
            )


class ServerStateDependencyManager:
    """Manages server state instantiation with dependency tracking."""

    def __init__(self, db_manager: DatabaseManager):
        self.db = db_manager
        self.sfsso_queue = SFSOQueueManager()
        self.pi_calculator = PIRationalRectificationCalculator()
        self.dependency_graph: Dict[str, List[str]] = {}
        self.resolved_states: Dict[str, Dict[str, Any]] = {}

    def instantiate_server_state(
        self,
        chain_id: str,
        server_id: str,
        state_data: Dict[str, Any],
        dependencies: List[str],
    ) -> Dict[str, Any]:
        resolved_deps = self._resolve_dependencies(chain_id, dependencies)
        served_order = self.sfsso_queue.enqueue(chain_id, server_id, state_data)
        complete_state = {
            "chain_id": chain_id,
            "server_id": server_id,
            "state_data": state_data,
            "dependencies": dependencies,
            "resolved_dependencies": resolved_deps,
            "served_order": served_order,
            "instantiated_at": datetime.now(timezone.utc).isoformat(),
        }
        self.db.save_server_state(chain_id, server_id, state_data, dependencies, served_order)
        self.resolved_states[f"{chain_id}:{server_id}"] = complete_state
        return complete_state

    def _resolve_dependencies(self, chain_id: str, dependencies: List[str]) -> Dict[str, Any]:
        resolved: Dict[str, Any] = {}
        for dep in dependencies:
            if dep in self.resolved_states:
                resolved[dep] = self.resolved_states[dep]
            else:
                resolved[dep] = {"status": "pending", "dependency": dep}
        return resolved

    def process_sfsso_queue(self, chain_id: str) -> List[Dict[str, Any]]:
        processed: List[Dict[str, Any]] = []
        while True:
            item = self.sfsso_queue.dequeue(chain_id)
            if not item:
                break
            processed.append(item)
        return processed


class ProductionChainContextSystem:
    """Complete system with SFSSO, DB persistence, server-state instantiation, and PI rectification."""

    def __init__(self, db_path: str = "production_context.db"):
        self.db = DatabaseManager(db_path)
        self.calculator = ProductionChainCalculator()
        self.buffer = PermanentContextBuffer(self.db)
        self.server_manager = ServerStateDependencyManager(self.db)
        self.pi_calculator = PIRationalRectificationCalculator()
        self.operation_history: List[Dict[str, Any]] = []
        self.context_snapshots: Dict[str, Dict[str, Any]] = {}

    def initialize_chain_from_prod(
        self,
        chain_id: str,
        user_id: str,
        prod_systems: List[Tuple[str, str]],
        base_thresholds: Dict[str, float],
    ) -> Dict[str, Any]:
        chain = self.calculator.create_production_chain(chain_id, user_id)
        system_ids: List[str] = []

        for system_id, system_type_str in prod_systems:
            system_type = ProductionSystemType[system_type_str.upper()]
            unit = ProductionUnit(system_id, system_type)
            self.calculator.register_production_system(unit)
            system_ids.append(system_id)

        calculated = self.calculator.calculate_chain_from_production(
            chain_id=chain_id,
            production_unit_ids=system_ids,
            thresholds=base_thresholds,
        )

        init_context = {
            "chain_id": chain_id,
            "user_id": user_id,
            "initialization_time": datetime.now(timezone.utc).isoformat(),
            "production_systems": system_ids,
            "calculated_properties": calculated,
            "status": "initialized",
        }

        self.context_snapshots[chain_id] = init_context
        snapshot_key = self.buffer.save_snapshot(
            chain_id=chain_id,
            context_state=init_context,
            prod_selection="all",
            chain_movement="initialization",
        )

        self.operation_history.append(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "operation": "initialize_chain",
                "chain_id": chain_id,
                "snapshot_key": snapshot_key,
            }
        )
        return init_context

    def instantiate_server_states(self, chain_id: str, server_configs: List[Dict[str, Any]]) -> Dict[str, Any]:
        results: List[Dict[str, Any]] = []

        for config in server_configs:
            server_id = config.get("server_id")
            state_data = config.get("state_data", {})
            dependencies = config.get("dependencies", [])

            result = self.server_manager.instantiate_server_state(
                chain_id,
                server_id,
                state_data,
                dependencies,
            )
            results.append(result)

        return {
            "chain_id": chain_id,
            "servers_instantiated": len(results),
            "server_states": results,
            "sfsso_queue_status": self.server_manager.sfsso_queue.get_queue_status(chain_id),
        }

    def process_model_response_in_chain(
        self,
        chain_id: str,
        response_text: str,
        response_data: Dict[str, Any],
        model_id: str = "default",
    ) -> Dict[str, Any]:
        chain = self.calculator.chains.get(chain_id)
        if not chain:
            return {"error": f"Chain {chain_id} not found"}

        model_response = chain.record_model_response(
            response_text=response_text,
            response_data=response_data,
            model_id=model_id,
            confidence=0.85,
        )

        commodity_trial = response_data.get("commodity_trial", "medium")
        raw_pi = 3.14159
        rectified_pi, irrational_rect = self.pi_calculator.rectify_pi_value(
            raw_pi,
            commodity_trial,
            trial_intensity=1.0,
        )

        self.db.save_pi_calculation(
            chain_id,
            "model_response",
            rectified_pi,
            commodity_trial,
            irrational_rect,
        )

        if chain_id in self.context_snapshots:
            self.context_snapshots[chain_id]["model_response"] = {
                "timestamp": model_response["timestamp"],
                "model_id": model_response["model_id"],
                "pi_rectified": rectified_pi,
                "irrational_rectification": irrational_rect,
                "text_to_class_terminologies": model_response["text_to_class_terminologies"],
                "response_classes": self._extract_response_classes(response_data),
            }

        snapshot_key = self.buffer.save_snapshot(
            chain_id=chain_id,
            context_state=self.context_snapshots[chain_id],
            prod_selection="all",
            chain_movement="model_response",
        )

        self.operation_history.append(
            {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "operation": "process_model_response",
                "chain_id": chain_id,
                "pi_rectified": rectified_pi,
                "irrational_rectification": irrational_rect,
                "snapshot_key": snapshot_key,
            }
        )

        return {
            "chain_id": chain_id,
            "model_response_processed": True,
            "pi_rectified": rectified_pi,
            "irrational_rectification": irrational_rect,
            "snapshot_key": snapshot_key,
        }

    def _extract_response_classes(self, response_data: Dict[str, Any]) -> List[str]:
        classes: List[str] = []
        if not isinstance(response_data, dict):
            return classes
        for key, value in response_data.items():
            if isinstance(value, str):
                classes.append(f"{key}:{value}")
            elif isinstance(value, list):
                classes.extend([f"{key}:{item}" for item in value])
        return classes

    def process_sfsso_results(self, chain_id: str) -> Dict[str, Any]:
        processed = self.server_manager.process_sfsso_queue(chain_id)
        return {
            "chain_id": chain_id,
            "sfsso_processed_count": len(processed),
            "processed_items": processed,
            "processing_time": datetime.now(timezone.utc).isoformat(),
        }

    def export_complete_system(self, output_dir: str) -> None:
        os.makedirs(output_dir, exist_ok=True)

        self.calculator.export_permanent_buffer(f"{output_dir}/calculator_buffer.json")
        self.buffer.export_buffer(f"{output_dir}/context_buffer.json")

        with open(f"{output_dir}/operation_history.json", "w", encoding="utf-8") as handle:
            json.dump(
                {
                    "exported_at": datetime.now(timezone.utc).isoformat(),
                    "total_operations": len(self.operation_history),
                    "operations": self.operation_history,
                },
                handle,
                indent=2,
            )

        with open(f"{output_dir}/active_contexts.json", "w", encoding="utf-8") as handle:
            json.dump(
                {
                    "exported_at": datetime.now(timezone.utc).isoformat(),
                    "active_chains": len(self.context_snapshots),
                    "snapshots": self.context_snapshots,
                },
                handle,
                indent=2,
            )

        print(f"Complete system exported to {output_dir}/")

    def print_system_summary(self) -> None:
        print("\n=== Production Chain Context System Summary ===")
        print(f"Active chains: {len(self.context_snapshots)}")
        print(f"Total operations: {len(self.operation_history)}")
        print(f"Buffer snapshots: {len(self.buffer.buffer_store)}")
        print(f"Registered systems: {len(self.calculator.system_registry)}")

        for chain_id, snapshots in self.buffer.snapshots.items():
            print(f"\n  Chain: {chain_id}")
            print(f"  - Snapshots: {len(snapshots)}")
            if chain_id in self.context_snapshots:
                print("  - Status: active")


if __name__ == "__main__":
    system = ProductionChainContextSystem()
    print("=== Production Chain Context System (Enhanced) ===\n")

    chain_id = "PROD-CONTEXT-CHAIN-001"
    prod_systems = [
        ("ehr-001", "EHR_SYSTEM"),
        ("monitor-001", "MONITORING_SYSTEM"),
        ("diag-001", "DIAGNOSTIC_SYSTEM"),
        ("model-001", "MODEL_INFERENCE"),
        ("diag-002", "MODEL_CALL"),
        ("frame-power", "PERSONAL_RECOGNITION"),
    ]

    print("1. Initializing chain from production systems...")
    system.initialize_chain_from_prod(
        chain_id=chain_id,
        user_id="ER-PHYSICIAN-001",
        prod_systems=prod_systems,
        base_thresholds={
            "duration_critical": 2.0,
            "duration_warning": 1.0,
            "fluency_min": 0.85,
        },
    )
    print("✓ Chain initialized")

    print("\n2. Instantiating server states with SFSSO...")
    server_configs = [
        {
            "server_id": "server-001",
            "state_data": {"status": "active", "capacity": 100},
            "dependencies": [],
        },
        {
            "server_id": "server-002",
            "state_data": {"status": "standby", "capacity": 50},
            "dependencies": ["server-001"],
        },
    ]
    server_result = system.instantiate_server_states(chain_id, server_configs)
    print(f"✓ Servers instantiated: {server_result['servers_instantiated']}")
    print(f"✓ SFSSO queue status: {server_result['sfsso_queue_status']}")

    print("\n3. Processing model response with PI rectification...")
    response = system.process_model_response_in_chain(
        chain_id=chain_id,
        response_text="CRITICAL: Septic shock detected.",
        response_data={
            "diagnosis": "septic_shock",
            "severity": "critical",
            "commodity_trial": "high",
        },
        model_id="clinical-ai-v2.1",
    )
    print(f"✓ Model response processed")
    print(f"✓ PI rectified: {response['pi_rectified']:.6f}")
    print(f"✓ Irrational rectification: {response['irrational_rectification']:.6f}")

    print("\n4. Processing SFSSO queue...")
    sfsso_result = system.process_sfsso_results(chain_id)
    print(f"✓ SFSSO items processed: {sfsso_result['sfsso_processed_count']}")

    print("\n5. Exporting complete system...")
    system.export_complete_system("results/production_context_system")
    system.print_system_summary()

    print("\nProduction chain context system completed")
