# OpenAPI\Client\DefaultApi

All URIs are relative to https://127.0.0.1:9333, except if the operation defines another base path.

| Method | HTTP request | Description |
| ------------- | ------------- | ------------- |
| [**dirAssign()**](DefaultApi.md#dirAssign) | **GET** /dir/assign | Assign a file key |
| [**dirLookup()**](DefaultApi.md#dirLookup) | **GET** /dir/lookup | Lookup volume |


## `dirAssign()`

```php
dirAssign($count, $collection, $data_center, $rack, $data_node, $disk, $replication, $ttl, $preallocate, $memory_map_max_size_mb, $writable_volume_count): \OpenAPI\Client\Model\FileKey
```

Assign a file key

This operation is very cheap. Just increase a number in master server's memory.

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');



$apiInstance = new OpenAPI\Client\Api\DefaultApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client()
);
$count = NULL; // mixed | how many file ids to assign. Use <fid>_1, <fid>_2 for the assigned additional file ids. e.g. 3,01637037d6_1, 3,01637037d6_2
$collection = NULL; // mixed | required collection name
$data_center = NULL; // mixed | preferred data center
$rack = NULL; // mixed | preferred rack
$data_node = NULL; // mixed | preferred volume server, e.g. 127.0.0.1:8080
$disk = NULL; // mixed | If you have disks labelled, this must be supplied to specify the disk type to allocate on.
$replication = NULL; // mixed | replica placement strategy
$ttl = NULL; // mixed | file expiration time limit, example: 3m for 3 minutes. units: m-minute, h-hour, d-day, w-week, M-month, y-year
$preallocate = NULL; // mixed | If no matching volumes, pre-allocate this number of bytes on disk for new volumes.
$memory_map_max_size_mb = NULL; // mixed | Only implemented for windows. Use memory mapped files with specified size for new volumes.
$writable_volume_count = NULL; // mixed | If no matching volumes, create specified number of new volumes.

try {
    $result = $apiInstance->dirAssign($count, $collection, $data_center, $rack, $data_node, $disk, $replication, $ttl, $preallocate, $memory_map_max_size_mb, $writable_volume_count);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling DefaultApi->dirAssign: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **count** | [**mixed**](../Model/.md)| how many file ids to assign. Use &lt;fid&gt;_1, &lt;fid&gt;_2 for the assigned additional file ids. e.g. 3,01637037d6_1, 3,01637037d6_2 | [optional] |
| **collection** | [**mixed**](../Model/.md)| required collection name | [optional] |
| **data_center** | [**mixed**](../Model/.md)| preferred data center | [optional] |
| **rack** | [**mixed**](../Model/.md)| preferred rack | [optional] |
| **data_node** | [**mixed**](../Model/.md)| preferred volume server, e.g. 127.0.0.1:8080 | [optional] |
| **disk** | [**mixed**](../Model/.md)| If you have disks labelled, this must be supplied to specify the disk type to allocate on. | [optional] |
| **replication** | [**mixed**](../Model/.md)| replica placement strategy | [optional] |
| **ttl** | [**mixed**](../Model/.md)| file expiration time limit, example: 3m for 3 minutes. units: m-minute, h-hour, d-day, w-week, M-month, y-year | [optional] |
| **preallocate** | [**mixed**](../Model/.md)| If no matching volumes, pre-allocate this number of bytes on disk for new volumes. | [optional] |
| **memory_map_max_size_mb** | [**mixed**](../Model/.md)| Only implemented for windows. Use memory mapped files with specified size for new volumes. | [optional] |
| **writable_volume_count** | [**mixed**](../Model/.md)| If no matching volumes, create specified number of new volumes. | [optional] |

### Return type

[**\OpenAPI\Client\Model\FileKey**](../Model/FileKey.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)

## `dirLookup()`

```php
dirLookup($volume_id, $collection, $file_id, $read): mixed
```

Lookup volume

We would need to find out whether the volumes have moved.

### Example

```php
<?php
require_once(__DIR__ . '/vendor/autoload.php');



$apiInstance = new OpenAPI\Client\Api\DefaultApi(
    // If you want use custom http client, pass your client which implements `GuzzleHttp\ClientInterface`.
    // This is optional, `GuzzleHttp\Client` will be used as default.
    new GuzzleHttp\Client()
);
$volume_id = NULL; // mixed | volume id
$collection = NULL; // mixed | optionally to speed up the lookup
$file_id = NULL; // mixed | If provided, this returns the fileId location and a JWT to update or delete the file.
$read = NULL; // mixed | works together with \"fileId\", if read=yes, JWT is generated for reads.

try {
    $result = $apiInstance->dirLookup($volume_id, $collection, $file_id, $read);
    print_r($result);
} catch (Exception $e) {
    echo 'Exception when calling DefaultApi->dirLookup: ', $e->getMessage(), PHP_EOL;
}
```

### Parameters

| Name | Type | Description  | Notes |
| ------------- | ------------- | ------------- | ------------- |
| **volume_id** | [**mixed**](../Model/.md)| volume id | [optional] |
| **collection** | [**mixed**](../Model/.md)| optionally to speed up the lookup | [optional] |
| **file_id** | [**mixed**](../Model/.md)| If provided, this returns the fileId location and a JWT to update or delete the file. | [optional] |
| **read** | [**mixed**](../Model/.md)| works together with \&quot;fileId\&quot;, if read&#x3D;yes, JWT is generated for reads. | [optional] |

### Return type

**mixed**

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: `application/json`

[[Back to top]](#) [[Back to API list]](../../README.md#endpoints)
[[Back to Model list]](../../README.md#models)
[[Back to README]](../../README.md)
