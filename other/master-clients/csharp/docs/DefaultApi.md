# Org.OpenAPITools.Api.DefaultApi

All URIs are relative to *https://127.0.0.1:9333*

Method | HTTP request | Description
------------- | ------------- | -------------
[**DirAssign**](DefaultApi.md#dirassign) | **GET** /dir/assign | Assign a file key
[**DirLookup**](DefaultApi.md#dirlookup) | **GET** /dir/lookup | Lookup volume



## DirAssign

> FileKey DirAssign (Object count = null, Object collection = null, Object dataCenter = null, Object rack = null, Object dataNode = null, Object disk = null, Object replication = null, Object ttl = null, Object preallocate = null, Object memoryMapMaxSizeMb = null, Object writableVolumeCount = null)

Assign a file key

This operation is very cheap. Just increase a number in master server's memory.

### Example

```csharp
using System.Collections.Generic;
using System.Diagnostics;
using Org.OpenAPITools.Api;
using Org.OpenAPITools.Client;
using Org.OpenAPITools.Model;

namespace Example
{
    public class DirAssignExample
    {
        public static void Main()
        {
            Configuration.Default.BasePath = "https://127.0.0.1:9333";
            var apiInstance = new DefaultApi(Configuration.Default);
            var count = new Object(); // Object | how many file ids to assign. Use <fid>_1, <fid>_2 for the assigned additional file ids. e.g. 3,01637037d6_1, 3,01637037d6_2 (optional) 
            var collection = new Object(); // Object | required collection name (optional) 
            var dataCenter = new Object(); // Object | preferred data center (optional) 
            var rack = new Object(); // Object | preferred rack (optional) 
            var dataNode = new Object(); // Object | preferred volume server, e.g. 127.0.0.1:8080 (optional) 
            var disk = new Object(); // Object | If you have disks labelled, this must be supplied to specify the disk type to allocate on. (optional) 
            var replication = new Object(); // Object | replica placement strategy (optional) 
            var ttl = new Object(); // Object | file expiration time limit, example: 3m for 3 minutes. units: m-minute, h-hour, d-day, w-week, M-month, y-year (optional) 
            var preallocate = new Object(); // Object | If no matching volumes, pre-allocate this number of bytes on disk for new volumes. (optional) 
            var memoryMapMaxSizeMb = new Object(); // Object | Only implemented for windows. Use memory mapped files with specified size for new volumes. (optional) 
            var writableVolumeCount = new Object(); // Object | If no matching volumes, create specified number of new volumes. (optional) 

            try
            {
                // Assign a file key
                FileKey result = apiInstance.DirAssign(count, collection, dataCenter, rack, dataNode, disk, replication, ttl, preallocate, memoryMapMaxSizeMb, writableVolumeCount);
                Debug.WriteLine(result);
            }
            catch (ApiException e)
            {
                Debug.Print("Exception when calling DefaultApi.DirAssign: " + e.Message );
                Debug.Print("Status Code: "+ e.ErrorCode);
                Debug.Print(e.StackTrace);
            }
        }
    }
}
```

### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **count** | [**Object**](Object.md)| how many file ids to assign. Use &lt;fid&gt;_1, &lt;fid&gt;_2 for the assigned additional file ids. e.g. 3,01637037d6_1, 3,01637037d6_2 | [optional] 
 **collection** | [**Object**](Object.md)| required collection name | [optional] 
 **dataCenter** | [**Object**](Object.md)| preferred data center | [optional] 
 **rack** | [**Object**](Object.md)| preferred rack | [optional] 
 **dataNode** | [**Object**](Object.md)| preferred volume server, e.g. 127.0.0.1:8080 | [optional] 
 **disk** | [**Object**](Object.md)| If you have disks labelled, this must be supplied to specify the disk type to allocate on. | [optional] 
 **replication** | [**Object**](Object.md)| replica placement strategy | [optional] 
 **ttl** | [**Object**](Object.md)| file expiration time limit, example: 3m for 3 minutes. units: m-minute, h-hour, d-day, w-week, M-month, y-year | [optional] 
 **preallocate** | [**Object**](Object.md)| If no matching volumes, pre-allocate this number of bytes on disk for new volumes. | [optional] 
 **memoryMapMaxSizeMb** | [**Object**](Object.md)| Only implemented for windows. Use memory mapped files with specified size for new volumes. | [optional] 
 **writableVolumeCount** | [**Object**](Object.md)| If no matching volumes, create specified number of new volumes. | [optional] 

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
| **200** | successful operation |  -  |

[[Back to top]](#)
[[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## DirLookup

> Object DirLookup (Object volumeId = null, Object collection = null, Object fileId = null, Object read = null)

Lookup volume

We would need to find out whether the volumes have moved.

### Example

```csharp
using System.Collections.Generic;
using System.Diagnostics;
using Org.OpenAPITools.Api;
using Org.OpenAPITools.Client;
using Org.OpenAPITools.Model;

namespace Example
{
    public class DirLookupExample
    {
        public static void Main()
        {
            Configuration.Default.BasePath = "https://127.0.0.1:9333";
            var apiInstance = new DefaultApi(Configuration.Default);
            var volumeId = new Object(); // Object | volume id (optional) 
            var collection = new Object(); // Object | optionally to speed up the lookup (optional) 
            var fileId = new Object(); // Object | If provided, this returns the fileId location and a JWT to update or delete the file. (optional) 
            var read = new Object(); // Object | works together with \"fileId\", if read=yes, JWT is generated for reads. (optional) 

            try
            {
                // Lookup volume
                Object result = apiInstance.DirLookup(volumeId, collection, fileId, read);
                Debug.WriteLine(result);
            }
            catch (ApiException e)
            {
                Debug.Print("Exception when calling DefaultApi.DirLookup: " + e.Message );
                Debug.Print("Status Code: "+ e.ErrorCode);
                Debug.Print(e.StackTrace);
            }
        }
    }
}
```

### Parameters


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **volumeId** | [**Object**](Object.md)| volume id | [optional] 
 **collection** | [**Object**](Object.md)| optionally to speed up the lookup | [optional] 
 **fileId** | [**Object**](Object.md)| If provided, this returns the fileId location and a JWT to update or delete the file. | [optional] 
 **read** | [**Object**](Object.md)| works together with \&quot;fileId\&quot;, if read&#x3D;yes, JWT is generated for reads. | [optional] 

### Return type

**Object**

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


### HTTP response details
| Status code | Description | Response headers |
|-------------|-------------|------------------|
| **200** | successful operation |  -  |

[[Back to top]](#)
[[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)

