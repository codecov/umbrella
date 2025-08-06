# LCOV `partials_as_hits` Feature Implementation

## Overview

This document describes the implementation of the `partials_as_hits` feature for the LCOV parser in Codecov. This feature allows users to treat partially covered lines as fully covered (hits) instead of showing them as partial coverage, providing consistency with other coverage parsers like JaCoCo, Cobertura, and Go.

## Background

### What are Partial Branches?

Partial coverage occurs when a line has some branches covered but not all. For example:
- A line with an `if` statement where only one branch is executed
- LCOV reports this as `BRDA:10,0,0,1` and `BRDA:10,0,1,0` (1 branch hit, 1 branch missed)
- This results in coverage like `"1/2"` (1 branch covered out of 2 total branches)

### The Problem

Before this implementation, LCOV was the only major parser that didn't support the `partials_as_hits` configuration option. This created inconsistency for teams using multiple coverage formats or migrating between them.

## Implementation Details

### Configuration Schema

The feature is configured via YAML:

```yaml
parsers:
  lcov:
    partials_as_hits: true  # false by default
```

### Files Modified

1. **Schema Validation** (`/libs/shared/shared/validation/user_schema.py`)
   - Added LCOV parser configuration schema
   - Validates boolean `partials_as_hits` option

2. **Core Implementation** (`/apps/worker/services/report/languages/lcov.py`)
   - Added configuration reading from YAML
   - Implemented partials-to-hits conversion logic
   - Updated function signatures to pass configuration

3. **Test Coverage** (`/apps/worker/services/report/languages/tests/unit/test_lcov.py`)
   - Added comprehensive test cases for all scenarios
   - Tests enabled/disabled behavior and mixed coverage scenarios

4. **Schema Tests** (`/libs/shared/tests/unit/validation/test_validation.py`)
   - Added validation tests for LCOV parser configuration

### Implementation Logic

#### 1. Configuration Reading
```python
partials_as_hits = report_builder_session.yaml_field(
    ("parsers", "lcov", "partials_as_hits"), False
)
```

#### 2. Partial Detection and Conversion
```python
# Add partials_as_hits conversion
if partials_as_hits and branch_sum > 0 and branch_sum < branch_num:
    # This is a partial branch, convert to hit
    coverage = 1
    missing_branches = None  # Clear missing branches for hits
    # Keep coverage_type as branch to maintain proper branch counting
```

#### 3. Conversion Logic
- **Condition**: `branch_sum > 0 && branch_sum < branch_num` identifies partial coverage
- **Action**: Convert to `coverage = 1` while preserving `CoverageType.branch`
- **Preservation**: Full hits and complete misses remain unchanged

## Behavior Examples

### With `partials_as_hits: true`

| Original Coverage | Converted Coverage | Reason |
|------------------|-------------------|---------|
| `"1/2"` (partial) | `1` (hit) | Partial converted to hit |
| `"2/3"` (partial) | `1` (hit) | Partial converted to hit |
| `"2/2"` (full hit) | `"2/2"` | Full hit preserved |
| `"0/2"` (miss) | `"0/2"` | Miss preserved |

### With `partials_as_hits: false` (default)

| Coverage | Behavior |
|----------|----------|
| `"1/2"` | Remains as partial |
| `"2/2"` | Remains as full hit |
| `"0/2"` | Remains as miss |

## Test Cases

### 1. Partials as Hits Enabled
```python
def test_lcov_partials_as_hits_enabled(self):
    """Test that partial branches become hits when partials_as_hits=True"""
    # LCOV data with partial coverage (1/2 branches covered)
    # Expected: partial "1/2" becomes hit (1)
```

### 2. Partials as Hits Disabled (Default)
```python
def test_lcov_partials_as_hits_disabled(self):
    """Test that partials remain partial when partials_as_hits=False (default)"""
    # Expected: partial "1/2" remains as partial
```

### 3. Mixed Coverage Scenarios
```python
def test_lcov_partials_as_hits_mixed_coverage(self):
    """Test partials_as_hits with mixed hit/miss/partial scenarios"""
    # Tests combinations of full hits, misses, and partials
    # Only partials should be converted to hits
```

## Consistency with Other Parsers

This implementation follows the same pattern established by other parsers:

### JaCoCo
```python
if (coverage_type == CoverageType.branch 
    and branch_type(cov) == LineType.partial 
    and partials_as_hits):
    cov = 1
```

### Cobertura
```python
if (isinstance(coverage, str) 
    and not coverage[0] == "0" 
    and partials_as_hits):
    coverage = 1
```

### Go
```python
if partials_as_hits and line_type(cov_to_use) == LineType.partial:
    cov_to_use = 1
```

## Use Cases

### When to Enable `partials_as_hits: true`

1. **Conservative Coverage Reporting**: Teams that want to count any execution as "covered"
2. **Simplified Metrics**: Avoiding the complexity of partial coverage visualization
3. **Legacy Compatibility**: Matching behavior of older or simpler coverage tools
4. **Cross-Parser Consistency**: When using multiple coverage formats that should report similarly

### When to Keep `partials_as_hits: false` (Default)

1. **Detailed Branch Analysis**: Teams that want to see which specific branches aren't tested
2. **Quality Metrics**: Partial coverage indicates incomplete testing and helps identify gaps
3. **Comprehensive Testing**: Helps ensure all code paths are properly tested

## Migration Guide

### For Existing LCOV Users

No changes required. The feature defaults to `false`, preserving existing behavior.

### To Enable the Feature

Add to your `codecov.yml`:

```yaml
parsers:
  lcov:
    partials_as_hits: true
```

### Validation

The configuration is validated at upload time. Invalid configurations will be rejected with clear error messages.

## Technical Details

### LCOV Format Processing

1. **Branch Data**: LCOV uses `BRDA` lines to report branch coverage
   - Format: `BRDA:<line>,<block>,<branch>,<taken>`
   - Example: `BRDA:10,0,0,1` (line 10, block 0, branch 0, taken 1 time)

2. **Coverage Calculation**: 
   - `branch_sum = sum(br.values())` - total branches taken
   - `branch_num = len(br.values())` - total branches found
   - Result: `"1/2"`, `"2/2"`, `"0/3"`, etc.

3. **Conversion Logic**: Only converts true partials where some but not all branches are covered

### Performance Impact

- Minimal performance impact: single boolean check per branch line
- No additional memory overhead
- Consistent with existing parser patterns

## Testing

### Unit Tests Added

1. **Schema Validation Tests**
   - `test_validate_lcov_partials_as_hits_true()`
   - `test_validate_lcov_partials_as_hits_false()`

2. **Functionality Tests**
   - `test_lcov_partials_as_hits_enabled()`
   - `test_lcov_partials_as_hits_disabled()`
   - `test_lcov_partials_as_hits_mixed_coverage()`

### Test Data

Tests use realistic LCOV data with:
- Line coverage (`DA` lines)
- Branch coverage (`BRDA` lines) 
- Mixed scenarios (hits, misses, partials)

## Future Considerations

1. **Documentation Updates**: Update user-facing documentation to include LCOV in partials_as_hits examples
2. **Integration Tests**: Add end-to-end tests with actual LCOV files
3. **Performance Monitoring**: Monitor for any performance impact in production
4. **User Feedback**: Gather feedback from teams using the feature

## Conclusion

The LCOV `partials_as_hits` feature provides:
- **Consistency**: Aligns LCOV with other major coverage parsers
- **Flexibility**: Allows teams to choose their preferred coverage reporting style  
- **Backward Compatibility**: Preserves existing behavior by default
- **Quality**: Comprehensive test coverage and validation

This implementation follows established patterns and maintains the high quality standards expected in the Codecov codebase.