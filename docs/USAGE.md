# LangChain Duration Benchmarking - Usage Guide

## Overview

This system provides comprehensive benchmarking and monitoring for LangChain operations in Emergency Room (ER) contexts, where language fluency, protocol consistency, and real-time change detection are critical.

## Core Components

### 1. Configuration System

**Files:**
- `config/thresholds.json` - Performance thresholds and alert levels
- `config/protocols.json` - Protocol definitions and priorities

**Key Settings:**
- **Duration Thresholds**: Critical (>2s), Warning (>1s), Optimal (<0.5s)
- **Language Fluency**: Min score 0.85, warning at 0.9
- **Protocol Changes**: Max 10 changes/second, warning at 7
- **Detection Window**: 100ms for language shift detection

### 2. Measurement Module (`measure_duration.py`)

Tracks model-text protocol execution and performance metrics.

**Usage:**
```python
from benchmarks.lang-chain-duration.scripts.measure_duration import DurationMeasurer

measurer = DurationMeasurer()

# Record a measurement
measurement = measurer.record_measurement(
    protocol_id="MT-001",
    duration=0.45,
    changes_count=8
)

# Save to file
measurer.save_measurements()
measurer.print_summary()
```

**Outputs:**
- `metrics/model_text_performance.csv` - Duration and performance data
- Console summary with statistics

### 3. Change Tracking Module (`track_changes.py`)

Monitors protocol changes and their severity.

**Usage:**
```python
from benchmarks.lang-chain-duration.scripts.track_changes import ChangeTracker

tracker = ChangeTracker()

# Record protocol changes
tracker.record_protocol_change(
    from_protocol="MT-001",
    to_protocol="MT-002",
    reason="Emergency escalation"
)

# Analyze change rate
rate = tracker.calculate_change_rate(time_window_seconds=1.0)

# Get summary
tracker.print_summary()
tracker.export_changes('results/changes.json')
```

**Features:**
- Protocol change logging with timestamps
- Severity assessment (high/medium/low)
- Rapid change detection
- Change rate analysis

### 4. Language Shift Detection Module (`detect_language_shift.py`)

Detects language changes during chain operations.

**Detection Types:**
- **While-Action**: Shifts during active model execution
- **Mid-Chain**: Shifts while operation is still in progress

**Usage:**
```python
from benchmarks.lang-chain-duration.scripts.detect_language_shift import LanguageShiftDetector

detector = LanguageShiftDetector(sensitivity="high")

# Start operation
detector.start_chain_operation("OP-001")

# Detect while-action shift
shift = detector.detect_while_action_shift(
    operation_id="OP-001",
    current_language_state=0.84,
    previous_language_state=0.92
)

# Detect mid-chain shift
shift = detector.detect_mid_chain_shift(
    operation_id="OP-001",
    context_consistency=0.88
)

# End operation and report
detector.end_chain_operation("OP-001")
detector.print_summary()
```

**Sensitivity Levels:**
- **Low**: Threshold 0.25 (only major changes detected)
- **Medium**: Threshold 0.15 (balanced detection)
- **High**: Threshold 0.08 (sensitive detection for ER contexts)

## Workflow Example: ER Scenario

```python
from benchmarks.lang-chain-duration.scripts import (
    DurationMeasurer,
    ChangeTracker,
    LanguageShiftDetector
)

# Initialize all components
measurer = DurationMeasurer()
tracker = ChangeTracker()
detector = LanguageShiftDetector(sensitivity="high")

# Start emergency operation
operation_id = "ER-QUERY-2026-06-12-001"
detector.start_chain_operation(operation_id)

# Execute with protocol MT-002 (Emergency Response)
start_duration, result = measurer.measure_protocol_execution(
    protocol_id="MT-002",
    execution_func=query_er_database,
    query_text="Patient vitals"
)

# Record measurement
measurer.record_measurement("MT-002", start_duration, changes_count=15)

# Monitor for language shifts
detector.detect_while_action_shift(operation_id, 0.88, 0.92)
detector.detect_mid_chain_shift(operation_id, 0.89)

# Check for protocol changes
if need_protocol_switch:
    change = tracker.record_protocol_change("MT-002", "MT-003", "Stability achieved")

# Complete operation
detector.end_chain_operation(operation_id)

# Generate reports
measurer.save_measurements()
tracker.export_changes('results/changes.json')
detector.export_detections('results/detections.json')
```

## Performance Interpretation

### Duration Metrics
- **Optimal (<0.5s)**: Normal operation, no issues
- **Warning (0.5-1.0s)**: Monitor, may indicate load
- **Critical (>1.0s)**: Action required, ER protocols may be affected

### Language Fluency Scores
- **0.95+**: Excellent, minimal changes
- **0.90-0.95**: Good, acceptable for ER
- **0.85-0.90**: Fair, monitor closely
- **<0.85**: Poor, intervention needed

### Change Detection Severity
- **Critical**: Major language shift (>0.2 magnitude)
- **High**: Significant shift (0.12-0.2 magnitude)
- **Medium**: Notable shift (0.06-0.12 magnitude)
- **Low**: Minor shift (<0.06 magnitude)

## Best Practices

1. **Regular Monitoring**: Check thresholds.json regularly and adjust for your ER's specific needs
2. **Real-time Alerts**: Set up alerts for critical status changes
3. **Sync Math Controls**: Enable in config for accurate control synchronization
4. **Emergency Mode**: Use high sensitivity for critical ER operations
5. **Data Retention**: Archive results regularly for trend analysis

## Troubleshooting

### High Change Rate
- Reduce detection window (current: 100ms)
- Check protocol definitions for unnecessary switches
- Review sync math control settings

### Low Language Fluency
- Increase fluency_warning_threshold in config
- Check for rapid context switches
- Review model-text protocol compatibility

### Rapid Shifts Detected
- Enable emergency mode sensitivity
- Review change_detection.log for timing patterns
- Consider protocol optimization

## Output Files

- `metrics/model_text_performance.csv` - Performance time series
- `metrics/language_fluency.json` - Fluency tracking data
- `metrics/change_detection.log` - Real-time change log
- `results/changes.json` - Exported change analysis
- `results/detections.json` - Exported shift detections
