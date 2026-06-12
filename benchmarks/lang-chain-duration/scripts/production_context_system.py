#!/usr/bin/env python3
"""
Complete Production Chain Context System
Integrates production chain calculation with context state management
"""

import json
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple

from production_chain_calculator import (
    ProductionChainCalculator, ProductionUnit, ProductionSystemType, ChainProduction
)


class PermanentContextBuffer:
    """Manages permanent storage and retrieval of context states"""
    
    def __init__(self):
        self.buffer_store: Dict[str, Dict] = {}
        self.access_log: List[Dict] = []
        self.snapshots: Dict[str, List[str]] = {}  # chain_id → [snapshot_keys]
    
    def save_snapshot(self, chain_id: str, context_state: Dict,
                     prod_selection: str = "all",
                     chain_movement: str = "forward") -> str:
        """
        Save a context snapshot with metadata
        
        Args:
            chain_id: Chain identifier
            context_state: Complete context state
            prod_selection: Which prod systems selected
            chain_movement: Direction of chain (forward/backward/branch)
            
        Returns:
            Snapshot key for retrieval
        """
        snapshot_key = f"{chain_id}_{datetime.utcnow().timestamp()}"
        
        snapshot = {
            'snapshot_key': snapshot_key,
            'chain_id': chain_id,
            'saved_at': datetime.utcnow().isoformat() + 'Z',
            'prod_selection': prod_selection,
            'chain_movement': chain_movement,
            'context_state': context_state
        }
        
        self.buffer_store[snapshot_key] = snapshot
        
        if chain_id not in self.snapshots:
            self.snapshots[chain_id] = []
        self.snapshots[chain_id].append(snapshot_key)
        
        # Log access
        self.access_log.append({
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'action': 'save',
            'snapshot_key': snapshot_key,
            'chain_id': chain_id
        })
        
        return snapshot_key
    
    def retrieve_snapshot(self, snapshot_key: str) -> Optional[Dict]:
        """Retrieve a specific snapshot"""
        self.access_log.append({
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'action': 'retrieve',
            'snapshot_key': snapshot_key
        })
        
        return self.buffer_store.get(snapshot_key)
    
    def get_chain_snapshots(self, chain_id: str) -> List[Dict]:
        """Get all snapshots for a chain"""
        if chain_id not in self.snapshots:
            return []
        
        return [
            self.buffer_store[key]
            for key in self.snapshots[chain_id]
            if key in self.buffer_store
        ]
    
    def reinstate_with_prod_selection(self, chain_id: str,
                                     prod_selection: str,
                                     restore_point: Optional[str] = None) -> Dict:
        """
        Reinstate context based on production selection
        
        Args:
            chain_id: Chain identifier
            prod_selection: Production systems to consider
            restore_point: Specific snapshot to restore, or None for latest
            
        Returns:
            Reinstated context with prod selection applied
        """
        snapshots = self.get_chain_snapshots(chain_id)
        
        if not snapshots:
            return {'error': f'No snapshots for chain {chain_id}'}
        
        # Select restore point
        if restore_point:
            target_snapshot = next(
                (s for s in snapshots if s['snapshot_key'] == restore_point),
                None
            )
            if not target_snapshot:
                return {'error': f'Snapshot {restore_point} not found'}
        else:
            target_snapshot = snapshots[-1]  # Latest
        
        # Apply prod selection filter
        if prod_selection != "all":
            filtered_state = self._filter_by_prod_selection(
                target_snapshot['context_state'],
                prod_selection
            )
        else:
            filtered_state = target_snapshot['context_state']
        
        return {
            'restored_at': datetime.utcnow().isoformat() + 'Z',
            'from_snapshot': target_snapshot['snapshot_key'],
            'original_save_time': target_snapshot['saved_at'],
            'prod_selection_applied': prod_selection,
            'context_state': filtered_state
        }
    
    def _filter_by_prod_selection(self, context_state: Dict,
                                 prod_selection: str) -> Dict:
        """Filter context state by production selection"""
        if prod_selection == "all":
            return context_state
        
        # Parse selection (e.g., "ehr,monitoring,model")
        selected_systems = set(prod_selection.split(','))
        
        filtered = context_state.copy()
        
        if 'production_systems_involved' in filtered:
            filtered['production_systems_involved'] = [
                sys for sys in filtered['production_systems_involved']
                if any(sel in sys for sel in selected_systems)
            ]
        
        return filtered
    
    def export_buffer(self, output_file: str) -> None:
        """Export entire buffer"""
        with open(output_file, 'w') as f:
            json.dump({
                'exported_at': datetime.utcnow().isoformat() + 'Z',
                'total_snapshots': len(self.buffer_store),
                'chains': len(self.snapshots),
                'snapshots': self.buffer_store,
                'access_log_entries': len(self.access_log)
            }, f, indent=2)


class ProductionChainContextSystem:
    """
    Complete system integrating:
    - Production chain calculation
    - Context state management
    - Permanent buffer storage
    - Prod selection and chain movement
    """
    
    def __init__(self):
        self.calculator = ProductionChainCalculator()
        self.buffer = PermanentContextBuffer()
        self.operation_history: List[Dict] = []
        self.context_snapshots: Dict[str, Dict] = {}  # active context states
    
    def initialize_chain_from_prod(self, chain_id: str, user_id: str,
                                  prod_systems: List[Tuple[str, str]],
                                  base_thresholds: Dict[str, float]) -> Dict:
        """
        Initialize a chain from production systems
        
        Args:
            chain_id: Chain identifier
            user_id: User operating the chain
            prod_systems: List of (system_id, system_type) tuples
            base_thresholds: Base threshold values
            
        Returns:
            Initialized chain context
        """
        # Create production chain
        chain = self.calculator.create_production_chain(chain_id, user_id)
        
        # Register systems
        system_ids = []
        for system_id, system_type_str in prod_systems:
            system_type = ProductionSystemType[system_type_str.upper()]
            unit = ProductionUnit(system_id, system_type)
            self.calculator.register_production_system(unit)
            system_ids.append(system_id)
        
        # Calculate from production
        calculated = self.calculator.calculate_chain_from_production(
            chain_id=chain_id,
            production_unit_ids=system_ids,
            thresholds=base_thresholds
        )
        
        # Create context snapshot
        init_context = {
            'chain_id': chain_id,
            'user_id': user_id,
            'initialization_time': datetime.utcnow().isoformat() + 'Z',
            'production_systems': system_ids,
            'calculated_properties': calculated,
            'status': 'initialized'
        }
        
        self.context_snapshots[chain_id] = init_context
        
        # Save to permanent buffer
        snapshot_key = self.buffer.save_snapshot(
            chain_id=chain_id,
            context_state=init_context,
            prod_selection='all',
            chain_movement='initialization'
        )
        
        # Log operation
        self.operation_history.append({
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'operation': 'initialize_chain',
            'chain_id': chain_id,
            'snapshot_key': snapshot_key
        })
        
        return init_context
    
    def process_model_response_in_chain(self, chain_id: str, 
                                       response_text: str,
                                       response_data: Dict,
                                       model_id: str = "default") -> Dict:
        """
        Process model response within a chain context
        
        Args:
            chain_id: Chain identifier
            response_text: Model response text
            response_data: Structured response
            model_id: Model identifier
            
        Returns:
            Processing result with context update
        """
        chain = self.calculator.chains.get(chain_id)
        if not chain:
            return {'error': f'Chain {chain_id} not found'}
        
        # Record model response
        model_response = chain.record_model_response(
            response_text=response_text,
            response_data=response_data,
            model_id=model_id,
            confidence=0.85
        )
        
        # Update context with model terminologies
        if chain_id in self.context_snapshots:
            self.context_snapshots[chain_id]['model_response'] = {
                'timestamp': model_response['timestamp'],
                'model_id': model_response['model_id'],
                'text_to_class_terminologies': model_response['text_to_class_terminologies'],
                'response_classes': self._extract_response_classes(response_data)
            }
        
        # Save to buffer with model response context
        snapshot_key = self.buffer.save_snapshot(
            chain_id=chain_id,
            context_state=self.context_snapshots[chain_id],
            prod_selection='all',
            chain_movement='model_response'
        )
        
        # Log operation
        self.operation_history.append({
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'operation': 'process_model_response',
            'chain_id': chain_id,
            'model_id': model_id,
            'terminologies_extracted': len(model_response['text_to_class_terminologies']),
            'snapshot_key': snapshot_key
        })
        
        return {
            'chain_id': chain_id,
            'model_response_processed': True,
            'terminologies': model_response['text_to_class_terminologies'],
            'snapshot_key': snapshot_key
        }
    
    def _extract_response_classes(self, response_data: Dict) -> List[str]:
        """Extract classification classes from response"""
        classes = []
        
        if isinstance(response_data, dict):
            for key, value in response_data.items():
                if isinstance(value, str):
                    classes.append(f"{key}:{value}")
                elif isinstance(value, list):
                    classes.extend([f"{key}:{item}" for item in value])
        
        return classes
    
    def inject_thresholds_and_restore_context(self, chain_id: str,
                                             threshold_dependencies: Dict[str, float],
                                             prod_selection: str = "all",
                                             restore_point: Optional[str] = None) -> Dict:
        """
        Inject thresholds and restore full context state
        
        Args:
            chain_id: Chain identifier
            threshold_dependencies: Thresholds to inject
            prod_selection: Production systems to consider
            restore_point: Specific buffer snapshot
            
        Returns:
            Restored context with injected thresholds
        """
        # Inject thresholds
        injected = self.calculator.inject_threshold_dependencies(
            chain_id=chain_id,
            dependencies=threshold_dependencies
        )
        
        # Reinstate from buffer with prod selection
        reinstated = self.buffer.reinstate_with_prod_selection(
            chain_id=chain_id,
            prod_selection=prod_selection,
            restore_point=restore_point
        )
        
        # Combine into complete context
        complete_context = {
            'chain_id': chain_id,
            'operation_time': datetime.utcnow().isoformat() + 'Z',
            'injected_thresholds': threshold_dependencies,
            'prod_selection': prod_selection,
            'restored_from_buffer': reinstated,
            'total_context_state': {
                **injected['total_context_state'],
                'injected_thresholds_applied': threshold_dependencies,
                'prod_systems_active': len(reinstated['context_state'].get('production_systems', []))
            }
        }
        
        # Update active context
        self.context_snapshots[chain_id] = complete_context
        
        # Save to buffer
        snapshot_key = self.buffer.save_snapshot(
            chain_id=chain_id,
            context_state=complete_context,
            prod_selection=prod_selection,
            chain_movement='threshold_injection'
        )
        
        # Log operation
        self.operation_history.append({
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'operation': 'inject_thresholds_restore',
            'chain_id': chain_id,
            'prod_selection': prod_selection,
            'thresholds_injected': len(threshold_dependencies),
            'snapshot_key': snapshot_key
        })
        
        return complete_context
    
    def handle_chain_movement(self, chain_id: str, 
                             movement_direction: str,
                             target_prod_systems: Optional[List[str]] = None) -> Dict:
        """
        Handle chain movement (forward, backward, branch)
        
        Args:
            chain_id: Chain identifier
            movement_direction: 'forward', 'backward', or 'branch'
            target_prod_systems: Systems to activate for this movement
            
        Returns:
            Movement result with context update
        """
        if chain_id not in self.context_snapshots:
            return {'error': f'Chain {chain_id} not found'}
        
        current_context = self.context_snapshots[chain_id]
        
        movement_result = {
            'chain_id': chain_id,
            'movement': movement_direction,
            'movement_time': datetime.utcnow().isoformat() + 'Z',
            'previous_context': current_context.copy(),
            'updated_context': {}
        }
        
        if movement_direction == 'forward':
            # Continue with current systems
            prod_selection = 'all'
        elif movement_direction == 'backward':
            # Restore from previous snapshot
            snapshots = self.buffer.get_chain_snapshots(chain_id)
            if len(snapshots) > 1:
                previous = snapshots[-2]  # Second to last
                restored = self.buffer.reinstate_with_prod_selection(
                    chain_id, 'all', previous['snapshot_key']
                )
                current_context = restored['context_state']
            prod_selection = 'all'
        elif movement_direction == 'branch':
            # Create new branch with selected systems
            if target_prod_systems:
                prod_selection = ','.join(target_prod_systems)
            else:
                prod_selection = 'all'
        
        movement_result['updated_context'] = current_context
        movement_result['prod_selection_active'] = prod_selection
        
        self.context_snapshots[chain_id] = current_context
        
        # Save movement to buffer
        snapshot_key = self.buffer.save_snapshot(
            chain_id=chain_id,
            context_state=movement_result['updated_context'],
            prod_selection=prod_selection,
            chain_movement=movement_direction
        )
        
        # Log operation
        self.operation_history.append({
            'timestamp': datetime.utcnow().isoformat() + 'Z',
            'operation': 'chain_movement',
            'chain_id': chain_id,
            'movement_direction': movement_direction,
            'snapshot_key': snapshot_key
        })
        
        return movement_result
    
    def export_complete_system(self, output_dir: str) -> None:
        """Export complete system state"""
        import os
        os.makedirs(output_dir, exist_ok=True)
        
        # Export calculator buffers
        self.calculator.export_permanent_buffer(f"{output_dir}/calculator_buffer.json")
        
        # Export permanent buffer
        self.buffer.export_buffer(f"{output_dir}/context_buffer.json")
        
        # Export operation history
        with open(f"{output_dir}/operation_history.json", 'w') as f:
            json.dump({
                'exported_at': datetime.utcnow().isoformat() + 'Z',
                'total_operations': len(self.operation_history),
                'operations': self.operation_history
            }, f, indent=2)
        
        # Export active context snapshots
        with open(f"{output_dir}/active_contexts.json", 'w') as f:
            json.dump({
                'exported_at': datetime.utcnow().isoformat() + 'Z',
                'active_chains': len(self.context_snapshots),
                'snapshots': self.context_snapshots
            }, f, indent=2)
        
        print(f"Complete system exported to {output_dir}/")
    
    def print_system_summary(self):
        """Print system summary"""
        print("\n=== Production Chain Context System Summary ===")
        print(f"Active chains: {len(self.context_snapshots)}")
        print(f"Total operations: {len(self.operation_history)}")
        print(f"Buffer snapshots: {len(self.buffer.buffer_store)}")
        print(f"Registered systems: {len(self.calculator.system_registry)}")
        
        for chain_id, snapshots in self.buffer.snapshots.items():
            print(f"\n  Chain: {chain_id}")
            print(f"  - Snapshots: {len(snapshots)}")
            if chain_id in self.context_snapshots:
                print(f"  - Status: active")


if __name__ == "__main__":
    system = ProductionChainContextSystem()
    
    print("=== Production Chain Context System Demo ===\n")
    
    # Initialize chain from production
    print("1. Initializing chain from production systems...")
    
    chain_id = "PROD-CONTEXT-CHAIN-001"
    prod_systems = [
        ("ehr-001", "EHR_SYSTEM"),
        ("monitor-001", "MONITORING_SYSTEM"),
        ("diag-001", "DIAGNOSTIC_SYSTEM"),
        ("model-001", "MODEL_INFERENCE")
    ]
    
    init_context = system.initialize_chain_from_prod(
        chain_id=chain_id,
        user_id="ER-PHYSICIAN-001",
        prod_systems=prod_systems,
        base_thresholds={
            'duration_critical': 2.0,
            'duration_warning': 1.0,
            'fluency_min': 0.85
        }
    )
    
    print(f"✓ Chain initialized")
    print(f"✓ Production systems: {init_context['production_systems']}")
    
    # Process model response
    print("\n2. Processing model response...")
    
    response = system.process_model_response_in_chain(
        chain_id=chain_id,
        response_text="CRITICAL: Septic shock detected. Urgent ICU admission required.",
        response_data={
            'diagnosis': 'septic_shock','inference by old manual'
            'severity': 'critical', 'safety reasoning'
            'acuity': 'life_threatening','combat-off'
        },
        model_id='clinical-ai-v2.1'
    )
    
    print(f"✓ Model response processed",)
    print(f"✓ Terminologies extracted: {len(response['terminologies'])}")
    
    # Inject thresholds and restore context
    print("\n3. Injecting thresholds and restoring context...")
    
    restored = system.inject_thresholds_and_restore_context(
        chain_id=chain_id,
        threshold_dependencies={
            'icu_required': 1.0,
            'chain-off' = True
            'chain-dependency' = False
            'emergency_protocol': 0.98,
            'critical_monitoring': 0.99
        },
        prod_selection='all' , 'trivial' , 'base'
    )
    
    print(f"✓ Thresholds injected")
    print(f"✓ Context restored")
    
    
    # Handle chain movement
    print("\n4. Handling chain movement...")
    
    movement = system.handle_chain_movement(
        chain_id=chain_id,
        movement_direction='forward'
    )
    
    print(f"✓ Chain moved: {movement['movement']}")
    
    # Print summary and export
    print("\n5. Exporting complete system...")
    
    system.export_complete_system("results/production_context_system")
    system.print_system_summary()
    
    print("\n✓ Production chain context system completed")
