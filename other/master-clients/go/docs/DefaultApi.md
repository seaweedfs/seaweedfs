# \DefaultApi

All URIs are relative to *https://127.0.0.1:9333*

Method | HTTP request | Description
------------- | ------------- | -------------
[**DirAssign**](DefaultApi.md#DirAssign) | **Get** /dir/assign | Assign a file key
[**DirLookup**](DefaultApi.md#DirLookup) | **Get** /dir/lookup | Lookup volume



## DirAssign

> FileKey DirAssign(ctx).Count(count).Collection(collection).DataCenter(dataCenter).Rack(rack).DataNode(dataNode).Disk(disk).Replication(replication).Ttl(ttl).Preallocate(preallocate).MemoryMapMaxSizeMb(memoryMapMaxSizeMb).WritableVolumeCount(writableVolumeCount).Execute()

Assign a file key



### Example

```go
package main

import (
    "context"
    "fmt"
    "os"
    openapiclient "github.com/GIT_USER_ID/GIT_REPO_ID"
)

func main() {
    count := TODO // interface{} | how many file ids to assign. Use <fid>_1, <fid>_2 for the assigned additional file ids. e.g. 3,01637037d6_1, 3,01637037d6_2 (optional)
    collection := TODO // interface{} | required collection name (optional)
    dataCenter := TODO // interface{} | preferred data center (optional)
    rack := TODO // interface{} | preferred rack (optional)
    dataNode := TODO // interface{} | preferred volume server, e.g. 127.0.0.1:8080 (optional)
    disk := TODO // interface{} | If you have disks labelled, this must be supplied to specify the disk type to allocate on. (optional)
    replication := TODO // interface{} | replica placement strategy (optional)
    ttl := TODO // interface{} | file expiration time limit, example: 3m for 3 minutes. units: m-minute, h-hour, d-day, w-week, M-month, y-year (optional)
    preallocate := TODO // interface{} | If no matching volumes, pre-allocate this number of bytes on disk for new volumes. (optional)
    memoryMapMaxSizeMb := TODO // interface{} | Only implemented for windows. Use memory mapped files with specified size for new volumes. (optional)
    writableVolumeCount := TODO // interface{} | If no matching volumes, create specified number of new volumes. (optional)

    configuration := openapiclient.NewConfiguration()
    apiClient := openapiclient.NewAPIClient(configuration)
    resp, r, err := apiClient.DefaultApi.DirAssign(context.Background()).Count(count).Collection(collection).DataCenter(dataCenter).Rack(rack).DataNode(dataNode).Disk(disk).Replication(replication).Ttl(ttl).Preallocate(preallocate).MemoryMapMaxSizeMb(memoryMapMaxSizeMb).WritableVolumeCount(writableVolumeCount).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `DefaultApi.DirAssign``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `DirAssign`: FileKey
    fmt.Fprintf(os.Stdout, "Response from `DefaultApi.DirAssign`: %v\n", resp)
}
```

### Path Parameters



### Other Parameters

Other parameters are passed through a pointer to a apiDirAssignRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **count** | [**interface{}**](interface{}.md) | how many file ids to assign. Use &lt;fid&gt;_1, &lt;fid&gt;_2 for the assigned additional file ids. e.g. 3,01637037d6_1, 3,01637037d6_2 | 
 **collection** | [**interface{}**](interface{}.md) | required collection name | 
 **dataCenter** | [**interface{}**](interface{}.md) | preferred data center | 
 **rack** | [**interface{}**](interface{}.md) | preferred rack | 
 **dataNode** | [**interface{}**](interface{}.md) | preferred volume server, e.g. 127.0.0.1:8080 | 
 **disk** | [**interface{}**](interface{}.md) | If you have disks labelled, this must be supplied to specify the disk type to allocate on. | 
 **replication** | [**interface{}**](interface{}.md) | replica placement strategy | 
 **ttl** | [**interface{}**](interface{}.md) | file expiration time limit, example: 3m for 3 minutes. units: m-minute, h-hour, d-day, w-week, M-month, y-year | 
 **preallocate** | [**interface{}**](interface{}.md) | If no matching volumes, pre-allocate this number of bytes on disk for new volumes. | 
 **memoryMapMaxSizeMb** | [**interface{}**](interface{}.md) | Only implemented for windows. Use memory mapped files with specified size for new volumes. | 
 **writableVolumeCount** | [**interface{}**](interface{}.md) | If no matching volumes, create specified number of new volumes. | 

### Return type

[**FileKey**](FileKey.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)


## DirLookup

> interface{} DirLookup(ctx).VolumeId(volumeId).Collection(collection).FileId(fileId).Read(read).Execute()

Lookup volume



### Example

```go
package main

import (
    "context"
    "fmt"
    "os"
    openapiclient "github.com/GIT_USER_ID/GIT_REPO_ID"
)

func main() {
    volumeId := TODO // interface{} | volume id (optional)
    collection := TODO // interface{} | optionally to speed up the lookup (optional)
    fileId := TODO // interface{} | If provided, this returns the fileId location and a JWT to update or delete the file. (optional)
    read := TODO // interface{} | works together with \"fileId\", if read=yes, JWT is generated for reads. (optional)

    configuration := openapiclient.NewConfiguration()
    apiClient := openapiclient.NewAPIClient(configuration)
    resp, r, err := apiClient.DefaultApi.DirLookup(context.Background()).VolumeId(volumeId).Collection(collection).FileId(fileId).Read(read).Execute()
    if err != nil {
        fmt.Fprintf(os.Stderr, "Error when calling `DefaultApi.DirLookup``: %v\n", err)
        fmt.Fprintf(os.Stderr, "Full HTTP response: %v\n", r)
    }
    // response from `DirLookup`: interface{}
    fmt.Fprintf(os.Stdout, "Response from `DefaultApi.DirLookup`: %v\n", resp)
}
```

### Path Parameters



### Other Parameters

Other parameters are passed through a pointer to a apiDirLookupRequest struct via the builder pattern


Name | Type | Description  | Notes
------------- | ------------- | ------------- | -------------
 **volumeId** | [**interface{}**](interface{}.md) | volume id | 
 **collection** | [**interface{}**](interface{}.md) | optionally to speed up the lookup | 
 **fileId** | [**interface{}**](interface{}.md) | If provided, this returns the fileId location and a JWT to update or delete the file. | 
 **read** | [**interface{}**](interface{}.md) | works together with \&quot;fileId\&quot;, if read&#x3D;yes, JWT is generated for reads. | 

### Return type

**interface{}**

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

[[Back to top]](#) [[Back to API list]](../README.md#documentation-for-api-endpoints)
[[Back to Model list]](../README.md#documentation-for-models)
[[Back to README]](../README.md)

