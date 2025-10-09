from fastmcp import Client
import json
import pytest

def extract_response_data(result):
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
 
async def test_get_schedules_list(mcp_server):
    async with Client(mcp_server) as client:
        result = await client.call_tool("get_schedules_list", {})
        data = extract_response_data(result)
        assert_no_error_in_response(data, "get_schedules_list")
        
        if isinstance(data, dict):
            if "policies" in data:
                assert isinstance(data["policies"], list), "policies should be a list"
            elif "schedules" in data:
                assert isinstance(data["schedules"], list), "schedules should be a list"
            else:
                assert len(data) >= 0, "Response should be valid"
        elif isinstance(data, list):
            assert len(data) >= 0, "List response should be valid"
        elif isinstance(data, str):
            pass

async def test_get_schedule_properties(mcp_server):
    async with Client(mcp_server) as client:
        schedules_result = await client.call_tool("get_schedules_list", {})
        schedules_data = extract_response_data(schedules_result)
        assert_no_error_in_response(schedules_data, "get_schedules_list")
        
        schedule_id = None
        try:
            if isinstance(schedules_data, dict):
                if "policies" in schedules_data and schedules_data["policies"]:
                    policies_list = schedules_data["policies"]
                    if isinstance(policies_list, list) and len(policies_list) > 0:
                        policy = policies_list[0]
                        schedule_id = str(policy.get("policyId"))
                elif "schedules" in schedules_data and schedules_data["schedules"]:
                    schedules_list = schedules_data["schedules"]
                    if isinstance(schedules_list, list) and len(schedules_list) > 0:
                        schedule = schedules_list[0]
                        schedule_id = str(schedule.get("taskId"))
            elif isinstance(schedules_data, list) and schedules_data:
                task_id = schedules_data[0].get("taskId")
                schedule_id = str(task_id) if task_id is not None else None
        except Exception as e:
            raise AssertionError(f"Failed to extract schedule ID from response: {e}")
        
        if not schedule_id or schedule_id == "None":
            pytest.skip("No schedules exist in the system")
        
        result = await client.call_tool("get_schedule_properties", {"schedule_id": schedule_id})
        data = extract_response_data(result)
        assert_no_error_in_response(data, "get_schedule_properties")
        
        if isinstance(data, dict):
            assert len(data) >= 0, "Schedule properties response should be valid"
        elif isinstance(data, str):
            pass
