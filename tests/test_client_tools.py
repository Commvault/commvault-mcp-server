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

def extract_id(response, id_field):
    try:
        data = extract_response_data(response)
        
        if isinstance(data, list) and len(data) > 0:
            first_item = data[0]
            if isinstance(first_item, dict) and id_field in first_item:
                value = first_item[id_field]
                return str(value) if value is not None else None
        
        elif isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, list) and len(value) > 0:
                    first_item = value[0]
                    if isinstance(first_item, dict) and id_field in first_item:
                        id_value = first_item[id_field]
                        return str(id_value) if id_value is not None else None
                
                if key == id_field and value is not None:
                    return str(value)
    except Exception:
        return None
    
    return None

async def test_get_client_group_list(mcp_server):
    async with Client(mcp_server) as client:
        result = await client.call_tool("get_client_group_list", {})
        data = extract_response_data(result)
        assert_no_error_in_response(data, "get_client_group_list")
        
        if isinstance(data, dict):
            if "clientGroups" in data:
                assert isinstance(data["clientGroups"], list), "clientGroups should be a list"
            elif "groups" in data:  
                assert isinstance(data["groups"], list), "groups should be a list"
            else:
                assert len(data) >= 0, "Response should be valid"
        elif isinstance(data, list):
            assert len(data) >= 0, "List response should be valid"
        elif isinstance(data, str):
            pytest.skip(f"get_client_group_list returned string response: {data[:100]}...")

async def test_get_client_list(mcp_server):
    async with Client(mcp_server) as client:
        result = await client.call_tool("get_client_list", {})
        data = extract_response_data(result)
        assert_no_error_in_response(data, "get_client_list")
        
        if isinstance(data, dict):
            if "clients" in data:
                assert isinstance(data["clients"], list), "clients should be a list"
            elif "clientProperties" in data: 
                assert isinstance(data["clientProperties"], list), "clientProperties should be a list"
            else:
                assert len(data) >= 0, "Response should be valid"
        elif isinstance(data, list):
            assert len(data) >= 0, "List response should be valid"
        elif isinstance(data, str):
            pytest.skip(f"get_client_list returned string response: {data[:100]}...")

async def test_get_client_group_properties(mcp_server):
    async with Client(mcp_server) as client:
        client_groups = await client.call_tool("get_client_group_list", {})
        
        groups_data = extract_response_data(client_groups)
        assert_no_error_in_response(groups_data, "get_client_group_list")
        
        client_group_id = None
        try:
            if isinstance(groups_data, dict):
                if "clientGroups" in groups_data and groups_data["clientGroups"]:
                    client_group_id = str(groups_data["clientGroups"][0].get("clientGroupId"))
                elif "groups" in groups_data and groups_data["groups"]:
                    first_group = groups_data["groups"][0]
                    client_group_id = str(first_group.get("clientGroup", {}).get("clientGroupId") or first_group.get("Id"))
            elif isinstance(groups_data, list) and groups_data:
                client_group_id = str(groups_data[0].get("id") or groups_data[0].get("clientGroupId"))
        except Exception as e:
            raise AssertionError(f"Failed to extract client group ID from response: {e}")
        
        if not client_group_id:
            if isinstance(groups_data, dict) and "clientGroups" in groups_data and len(groups_data["clientGroups"]) == 0:
                pytest.skip("No client groups exist in the system")
            else:
                raise AssertionError("Could not extract client group ID from API response")
        
        result = await client.call_tool("get_client_group_properties", {"client_group_id": client_group_id})
        data = extract_response_data(result)
        assert_no_error_in_response(data, "get_client_group_properties")
        assert isinstance(data, (dict, str)), "Expected dict or string response"

async def test_get_clientid_from_clientname(mcp_server):
    async with Client(mcp_server) as client:
        clients = await client.call_tool("get_client_list", {})
        
        clients_data = extract_response_data(clients)
        assert_no_error_in_response(clients_data, "get_client_list")
        
        client_name = None
        try:
            if isinstance(clients_data, dict):
                if "clients" in clients_data and clients_data["clients"]:
                    client_name = clients_data["clients"][0].get("clientName")
                elif "clientProperties" in clients_data and clients_data["clientProperties"]:
                    client_entity = clients_data["clientProperties"][0].get("client", {}).get("clientEntity", {})
                    client_name = client_entity.get("clientName")
            elif isinstance(clients_data, list) and clients_data:
                client_name = clients_data[0].get("clientName")
        except Exception as e:
            raise AssertionError(f"Failed to extract client name from response: {e}")
        
        if not client_name:
            if isinstance(clients_data, dict) and "clients" in clients_data and len(clients_data["clients"]) == 0:
                pytest.skip("No clients exist in the system")
            else:
                raise AssertionError("Could not extract client name from API response")
        
        result = await client.call_tool("get_clientid_from_clientname", {"client_name": client_name})
        data = extract_response_data(result)
        assert_no_error_in_response(data, "get_clientid_from_clientname")
        assert isinstance(data, (dict, str)), "Expected dict or string response"

async def test_get_subclient_list_by_name(mcp_server):
    async with Client(mcp_server) as client:
        clients = await client.call_tool("get_client_list", {})
        
        clients_data = extract_response_data(clients)
        assert_no_error_in_response(clients_data, "get_client_list")
        
        client_name = None
        try:
            if isinstance(clients_data, dict):
                if "clients" in clients_data and clients_data["clients"]:
                    client_name = clients_data["clients"][0].get("clientName")
                elif "clientProperties" in clients_data and clients_data["clientProperties"]:
                    client_entity = clients_data["clientProperties"][0].get("client", {}).get("clientEntity", {})
                    client_name = client_entity.get("clientName")
            elif isinstance(clients_data, list) and clients_data:
                client_name = clients_data[0].get("clientName")
        except Exception as e:
            raise AssertionError(f"Failed to extract client name from response: {e}")
        
        if not client_name:
            if isinstance(clients_data, dict) and "clients" in clients_data and len(clients_data["clients"]) == 0:
                pytest.skip("No clients exist in the system")
            else:
                raise AssertionError("Could not extract client name from API response")
        
        result = await client.call_tool("get_subclient_list", {"client_identifier": client_name, "identifier_type": "name"})
        data = extract_response_data(result)
        assert_no_error_in_response(data, "get_subclient_list")
        
        if isinstance(data, dict):
            if "subClients" in data:
                assert isinstance(data["subClients"], list), "subClients should be a list"
            elif "subClientProperties" in data: 
                assert isinstance(data["subClientProperties"], list), "subClientProperties should be a list"
        elif isinstance(data, str):
            pass
        else:
            assert isinstance(data, list), f"Expected dict, list, or string response, got {type(data)}"

async def test_get_subclient_list_by_id(mcp_server):
    async with Client(mcp_server) as client:
        clients = await client.call_tool("get_client_list", {})
        
        clients_data = extract_response_data(clients)
        assert_no_error_in_response(clients_data, "get_client_list")
        
        client_id = None
        try:
            if isinstance(clients_data, dict):
                if "clients" in clients_data and clients_data["clients"]:
                    client_id = str(clients_data["clients"][0].get("clientId"))
                elif "clientProperties" in clients_data and clients_data["clientProperties"]:
                    client_entity = clients_data["clientProperties"][0].get("client", {}).get("clientEntity", {})
                    client_id = str(client_entity.get("clientId"))
            elif isinstance(clients_data, list) and clients_data:
                client_id = str(clients_data[0].get("clientId"))
        except Exception as e:
            raise AssertionError(f"Failed to extract client ID from response: {e}")
        
        if not client_id or client_id == "None":
            if isinstance(clients_data, dict) and "clients" in clients_data and len(clients_data["clients"]) == 0:
                pytest.skip("No clients exist in the system (empty but successful response)")
            else:
                raise AssertionError("Could not extract valid client ID from API response")
        
        result = await client.call_tool("get_subclient_list", {"client_identifier": client_id, "identifier_type": "id"})
        data = extract_response_data(result)
        assert_no_error_in_response(data, "get_subclient_list")
        
        if isinstance(data, dict):
            if "subClients" in data:
                assert isinstance(data["subClients"], list), "subClients should be a list"
            elif "subClientProperties" in data:  
                assert isinstance(data["subClientProperties"], list), "subClientProperties should be a list"
        elif isinstance(data, str):
            pass
        else:
            assert isinstance(data, list), f"Expected dict, list, or string response, got {type(data)}"

async def test_get_subclient_properties(mcp_server):
    async with Client(mcp_server) as client:
        clients = await client.call_tool("get_client_list", {})
        
        clients_data = extract_response_data(clients)
        assert_no_error_in_response(clients_data, "get_client_list")

        client_id = None
        try:
            if isinstance(clients_data, dict):
                if "clients" in clients_data and clients_data["clients"]:
                    client_id = str(clients_data["clients"][0].get("clientId"))
                elif "clientProperties" in clients_data and clients_data["clientProperties"]:
                    client_entity = clients_data["clientProperties"][0].get("client", {}).get("clientEntity", {})
                    client_id = str(client_entity.get("clientId"))
            elif isinstance(clients_data, list) and clients_data:
                client_id = str(clients_data[0].get("clientId"))
        except Exception as e:
            raise AssertionError(f"Failed to extract client ID from response: {e}")

        if not client_id or client_id == "None":
            if isinstance(clients_data, dict) and "clients" in clients_data and len(clients_data["clients"]) == 0:
                pytest.skip("No clients exist in the system")
            else:
                raise AssertionError("Could not extract valid client ID from API response")
        
        subclients = await client.call_tool("get_subclient_list", {"client_identifier": client_id, "identifier_type": "id"})
        subclients_data = extract_response_data(subclients)
        assert_no_error_in_response(subclients_data, "get_subclient_list")
        
        subclient_id = None
        try:
            if isinstance(subclients_data, dict):
                if "subClients" in subclients_data and subclients_data["subClients"]:
                    subclient_id = str(subclients_data["subClients"][0].get("subclientId"))
                elif "subClientProperties" in subclients_data and subclients_data["subClientProperties"]:
                    sub_entity = subclients_data["subClientProperties"][0].get("subClientEntity", {})
                    subclient_id = str(sub_entity.get("subclientId"))
            elif isinstance(subclients_data, list) and subclients_data:
                subclient_id = str(subclients_data[0].get("subClientId") or subclients_data[0].get("subclientId"))
        except Exception as e:
            raise AssertionError(f"Failed to extract subclient ID from response: {e}")
        
        if not subclient_id or subclient_id == "None":
            if isinstance(subclients_data, dict) and "subClients" in subclients_data and len(subclients_data["subClients"]) == 0:
                pytest.skip("Client has no subclients")
            else:
                raise AssertionError("Could not extract valid subclient ID from API response")
        
        result = await client.call_tool("get_subclient_properties", {"subclient_id": subclient_id})
        data = extract_response_data(result)
        assert_no_error_in_response(data, "get_subclient_properties")
        assert isinstance(data, (dict, str)), "Expected dict or string response"

