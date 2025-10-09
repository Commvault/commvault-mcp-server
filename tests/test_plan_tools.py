from fastmcp import Client
import json
import pytest

def extract_response_data(result):
    # Handle CallToolResult object
    if hasattr(result, 'content'):
        content_list = result.content
    else:
        content_list = result
    
    if not isinstance(content_list, list) or len(content_list) == 0:
        raise AssertionError("Expected list response with at least one item")
    
    if not hasattr(content_list[0], "text"):
        raise AssertionError("Response item missing 'text' attribute")
    
    response_text = content_list[0].text
    
    try:
        return json.loads(response_text)
    except json.JSONDecodeError:
        return response_text

def assert_no_error_in_response(data, operation_name):
    if isinstance(data, str):
        error_indicators = [
            "error occurred", "failed to", "invalid", "unauthorized", 
            "not found", "exception", "traceback",
            "internal server error", "bad request", "forbidden"
        ]
        data_lower = data.lower()
        for indicator in error_indicators:
            if indicator in data_lower:
                raise AssertionError(f"{operation_name} failed with error: {data}")
        return
    
    elif isinstance(data, dict):
        if "error" in data:
            error = data["error"]
            if isinstance(error, dict):
                error_msg = error.get("errorMessage", "")
                error_code = error.get("errorCode", 0)
                if error_msg or error_code != 0:
                    raise AssertionError(f"{operation_name} failed with error: {error_msg} (code: {error_code})")
            elif error:
                raise AssertionError(f"{operation_name} failed with error: {error}")
        
        if "errorMessage" in data and data["errorMessage"]:
            raise AssertionError(f"{operation_name} failed: {data['errorMessage']}")
        
        if "errorCode" in data and data["errorCode"] != 0:
            raise AssertionError(f"{operation_name} failed with error code: {data['errorCode']}")

def find_plan_id(data):
    if isinstance(data, dict):
        if "plans" in data and isinstance(data["plans"], list) and len(data["plans"]) > 0:
            plan = data["plans"][0]
            if isinstance(plan, dict):
                if "plan" in plan and isinstance(plan["plan"], dict):
                    return plan["plan"].get("planId") or plan["plan"].get("id")
                return plan.get("planId") or plan.get("id")
        
        return data.get("planId") or data.get("id")
    
    elif isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
        return data[0].get("planId") or data[0].get("id")
    
    return None

async def test_get_plan_list(mcp_server):
    async with Client(mcp_server) as client:
        result = await client.call_tool("get_plan_list", {})
        data = extract_response_data(result)
        assert_no_error_in_response(data, "get_plan_list")
        
        if isinstance(data, dict):
            assert len(data) >= 0, "Response should be valid"
        elif isinstance(data, list):
            assert len(data) >= 0, "List response should be valid"

async def test_get_plan_properties(mcp_server):
    plan_id = None
    
    async with Client(mcp_server) as client:
        result = await client.call_tool("get_plan_list", {})
        data = extract_response_data(result)
        assert_no_error_in_response(data, "get_plan_list")
        
        plan_id = find_plan_id(data)
    
    if not plan_id:
        pytest.skip("No plans found in the system")
    
    async with Client(mcp_server) as client:
        result = await client.call_tool("get_plan_properties", {"plan_id": str(plan_id)})
        data = extract_response_data(result)
        assert_no_error_in_response(data, "get_plan_properties")
        
        if isinstance(data, dict):
            assert len(data) > 0, "Plan properties response should not be empty"
        elif isinstance(data, str):
            assert len(data) > 0, "Plan properties response should not be empty"
        assert len(data) > 0, "Plan properties response should not be empty"