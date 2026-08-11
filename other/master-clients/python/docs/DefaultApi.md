# openapi_client.DefaultApi

All URIs are relative to *https://127.0.0.1:9333*

Method | HTTP request | Description
------------- | ------------- | -------------
[**dir_assign**](DefaultApi.md#dir_assign) | **GET** /dir/assign | Assign a file key
[**dir_lookup**](DefaultApi.md#dir_lookup) | **GET** /dir/lookup | Lookup volume


# **dir_assign**
> FileKey dir_assign(count=count, collection=collection, data_center=data_center, rack=rack, data_node=data_node, disk=disk, replication=replication, ttl=ttl, preallocate=preallocate, memory_map_max_size_mb=memory_map_max_size_mb, writable_volume_count=writable_volume_count)

Assign a file key

This operation is very cheap. Just increase a number in master server's memory.

### Example

```python
from __future__ import print_function
import time
import os
import openapi_client
from openapi_client.rest import ApiException
from pprint import pprint
# Defining the host is optional and defaults to https://127.0.0.1:9333
# See configuration.py for a list of all supported configuration parameters.
configuration = openapi_client.Configuration(
    host = "https://127.0.0.1:9333"
)


# Enter a context with an instance of the API client
with openapi_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = openapi_client.DefaultApi(api_client)
    count = None # object | how many file ids to assign. Use <fid>_1, <fid>_2 for the assigned additional file ids. e.g. 3,01637037d6_1, 3,01637037d6_2 (optional)
    collection = None # object | required collection name (optional)
    data_center = None # object | preferred data center (optional)
    rack = None # object | preferred rack (optional)
    data_node = None # object | preferred volume server, e.g. 127.0.0.1:8080 (optional)
    disk = None # object | If you have disks labelled, this must be supplied to specify the disk type to allocate on. (optional)
    replication = None # object | replica placement strategy (optional)
    ttl = None # object | file expiration time limit, example: 3m for 3 minutes. units: m-minute, h-hour, d-day, w-week, M-month, y-year (optional)
    preallocate = None # object | If no matching volumes, pre-allocate this number of bytes on disk for new volumes. (optional)
    memory_map_max_size_mb = None # object | Only implemented for windows. Use memory mapped files with specified size for new volumes. (optional)
    writable_volume_count = None # object | If no matching volumes, create specified number of new volumes. (optional)

    try:
        # Assign a file key
        api_response = api_instance.dir_assign(count=count, collection=collection, data_center=data_center, rack=rack, data_node=data_node, disk=disk, replication=replication, ttl=ttl, preallocate=preallocate, memory_map_max_size_mb=memory_map_max_size_mb, writable_volume_count=writable_volume_count)
        print("The response of DefaultApi->dir_assign:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->dir_assign: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **count** | [**object**](.md)| how many file ids to assign. Use &lt;fid&gt;_1, &lt;fid&gt;_2 for the assigned additional file ids. e.g. 3,01637037d6_1, 3,01637037d6_2 | [optional] 
 **collection** | [**object**](.md)| required collection name | [optional] 
 **data_center** | [**object**](.md)| preferred data center | [optional] 
 **rack** | [**object**](.md)| preferred rack | [optional] 
 **data_node** | [**object**](.md)| preferred volume server, e.g. 127.0.0.1:8080 | [optional] 
 **disk** | [**object**](.md)| If you have disks labelled, this must be supplied to specify the disk type to allocate on. | [optional] 
 **replication** | [**object**](.md)| replica placement strategy | [optional] 
 **ttl** | [**object**](.md)| file expiration time limit, example: 3m for 3 minutes. units: m-minute, h-hour, d-day, w-week, M-month, y-year | [optional] 
 **preallocate** | [**object**](.md)| If no matching volumes, pre-allocate this number of bytes on disk for new volumes. | [optional] 
 **memory_map_max_size_mb** | [**object**](.md)| Only implemented for windows. Use memory mapped files with specified size for new volumes. | [optional] 
 **writable_volume_count** | [**object**](.md)| If no matching volumes, create specified number of new volumes. | [optional] 

### Return type

[**FileKey**](FileKey.md)

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful operation |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

# **dir_lookup**
> object dir_lookup(volume_id=volume_id, collection=collection, file_id=file_id, read=read)

Lookup volume

We would need to find out whether the volumes have moved.

### Example

```python
from __future__ import print_function
import time
import os
import openapi_client
from openapi_client.rest import ApiException
from pprint import pprint
# Defining the host is optional and defaults to https://127.0.0.1:9333
# See configuration.py for a list of all supported configuration parameters.
configuration = openapi_client.Configuration(
    host = "https://127.0.0.1:9333"
)


# Enter a context with an instance of the API client
with openapi_client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = openapi_client.DefaultApi(api_client)
    volume_id = None # object | volume id (optional)
    collection = None # object | optionally to speed up the lookup (optional)
    file_id = None # object | If provided, this returns the fileId location and a JWT to update or delete the file. (optional)
    read = None # object | works together with \"fileId\", if read=yes, JWT is generated for reads. (optional)

    try:
        # Lookup volume
        api_response = api_instance.dir_lookup(volume_id=volume_id, collection=collection, file_id=file_id, read=read)
        print("The response of DefaultApi->dir_lookup:\n")
        pprint(api_response)
    except Exception as e:
        print("Exception when calling DefaultApi->dir_lookup: %s\n" % e)
```

### Parameters

Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **volume_id** | [**object**](.md)| volume id | [optional] 
 **collection** | [**object**](.md)| optionally to speed up the lookup | [optional] 
 **file_id** | [**object**](.md)| If provided, this returns the fileId location and a JWT to update or delete the file. | [optional] 
 **read** | [**object**](.md)| works together with \&quot;fileId\&quot;, if read&#x3D;yes, JWT is generated for reads. | [optional] 

### Return type

**object**

### Authorization

No authorization required

### HTTP request headers

 - **Content-Type**: Not defined
 - **Accept**: application/json

### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
**200** | successful operation |  -  |

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to Model list]](../README.md#documentation-for-models) [[Back to README]](../README.md)

