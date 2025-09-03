#!/usr/bin/env python3
"""
Test script for FDA Food Enforcement Connector
This script validates the connector structure and basic functionality.
"""

import json
import sys
from datetime import datetime, timezone

def test_schema():
    """Test the schema function"""
    print("Testing schema function...")
    
    # Import the schema function
    from connector import schema
    
    # Test schema definition
    schema_result = schema({})
    
    # Validate schema structure
    assert isinstance(schema_result, list), "Schema should return a list"
    assert len(schema_result) > 0, "Schema should contain at least one table"
    
    # Check table structure
    table = schema_result[0]
    assert "table" in table, "Table definition should have 'table' field"
    assert "primary_key" in table, "Table definition should have 'primary_key' field"
    assert table["table"] == "food_enforcement_records", "Table name should be 'food_enforcement_records'"
    assert table["primary_key"] == ["recall_number"], "Primary key should be 'recall_number'"
    
    print("✓ Schema function test passed")
    return True

def test_data_flattening():
    """Test data flattening function"""
    print("Testing data flattening...")
    
    from connector import flatten_dict
    
    # Sample nested FDA API record
    sample_record = {
        "recall_number": "F-1234-2024",
        "recalling_firm": "Test Company Inc.",
        "openfda": {
            "brand_name": ["Test Brand"],
            "manufacturer_name": ["Test Manufacturer"]
        },
        "upc_codes": ["123456789", "987654321"],
        "classification": "Class I"
    }
    
    # Flatten the record
    flattened = flatten_dict(sample_record)
    
    # Validate flattening
    assert flattened["recall_number"] == "F-1234-2024", "Recall number should be preserved"
    assert flattened["openfda_brand_name"] == '["Test Brand"]', "Nested dict should be flattened"
    assert flattened["openfda_manufacturer_name"] == '["Test Manufacturer"]', "Nested dict should be flattened"
    assert flattened["upc_codes"] == '["123456789", "987654321"]', "List should be JSON string"
    assert flattened["classification"] == "Class I", "Simple field should be preserved"
    
    print("✓ Data flattening test passed")
    return True

def test_configuration():
    """Test configuration file"""
    print("Testing configuration...")
    
    try:
        with open("configuration.json", "r") as f:
            config = json.load(f)
        
        # Validate required fields
        required_fields = ["api_key", "limit", "max_records", "sync_mode"]
        for field in required_fields:
            assert field in config, f"Configuration should have '{field}' field"
        
        # Validate field types (all should be strings)
        for field, value in config.items():
            assert isinstance(value, str), f"Configuration field '{field}' should be a string"
        
        print("✓ Configuration test passed")
        return True
        
    except FileNotFoundError:
        print("✗ Configuration file not found")
        return False
    except json.JSONDecodeError:
        print("✗ Configuration file is not valid JSON")
        return False

def test_validation():
    """Test configuration validation"""
    print("Testing configuration validation...")
    
    from connector import validate_configuration
    
    # Test valid configuration
    valid_config = {
        "api_key": "test_key",
        "limit": "1000",
        "max_records": "10000",
        "sync_mode": "incremental"
    }
    
    try:
        validate_configuration(valid_config)
        print("✓ Valid configuration test passed")
    except Exception as e:
        print(f"✗ Valid configuration test failed: {str(e)}")
        return False
    
    # Test invalid configuration (missing api_key)
    invalid_config = {
        "limit": "1000",
        "max_records": "10000",
        "sync_mode": "incremental"
    }
    
    try:
        validate_configuration(invalid_config)
        print("✗ Invalid configuration test failed - should have raised ValueError")
        return False
    except ValueError:
        print("✓ Invalid configuration test passed")
    except Exception as e:
        print(f"✗ Invalid configuration test failed with unexpected error: {str(e)}")
        return False
    
    return True

def main():
    """Run all tests"""
    print("FDA Food Enforcement Connector - Test Suite")
    print("=" * 50)
    
    tests = [
        test_schema,
        test_data_flattening,
        test_configuration,
        test_validation
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"✗ {test.__name__} failed: {str(e)}")
    
    print("=" * 50)
    print(f"Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Connector is ready for use.")
        print("\nNext steps:")
        print("1. Get your FDA API key from https://open.fda.gov/apis/")
        print("2. Update configuration.json with your API key")
        print("3. Test with: fivetran debug --configuration configuration.json")
        return 0
    else:
        print("❌ Some tests failed. Please review the issues above.")
        return 1

if __name__ == "__main__":
    sys.exit(main())
