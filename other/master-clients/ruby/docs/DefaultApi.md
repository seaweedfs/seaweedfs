# OpenapiClient::DefaultApi

All URIs are relative to *https://127.0.0.1:9333*

| Method | HTTP request | Description |
| ------ | ------------ | ----------- |
| [**dir_assign**](DefaultApi.md#dir_assign) | **GET** /dir/assign | Assign a file key |
| [**dir_lookup**](DefaultApi.md#dir_lookup) | **GET** /dir/lookup | Lookup volume |


## dir_assign

> <FileKey> dir_assign(opts)

Assign a file key

This operation is very cheap. Just increase a number in master server's memory.

### Examples

```ruby
require 'time'
require 'openapi_client'

api_instance = OpenapiClient::DefaultApi.new
opts = {
  count: TODO, # Object | how many file ids to assign. Use <fid>_1, <fid>_2 for the assigned additional file ids. e.g. 3,01637037d6_1, 3,01637037d6_2
  collection: TODO, # Object | required collection name
  data_center: TODO, # Object | preferred data center
  rack: TODO, # Object | preferred rack
  data_node: TODO, # Object | preferred volume server, e.g. 127.0.0.1:8080
  disk: TODO, # Object | If you have disks labelled, this must be supplied to specify the disk type to allocate on.
  replication: TODO, # Object | replica placement strategy
  ttl: TODO, # Object | file expiration time limit, example: 3m for 3 minutes. units: m-minute, h-hour, d-day, w-week, M-month, y-year
  preallocate: TODO, # Object | If no matching volumes, pre-allocate this number of bytes on disk for new volumes.
  memory_map_max_size_mb: TODO, # Object | Only implemented for windows. Use memory mapped files with specified size for new volumes.
  writable_volume_count: TODO # Object | If no matching volumes, create specified number of new volumes.
}

begin
  # Assign a file key
  result = api_instance.dir_assign(opts)
  p result
rescue OpenapiClient::ApiError => e
  puts "Error when calling DefaultApi->dir_assign: #{e}"
end
```

#### Using the dir_assign_with_http_info variant

This returns an Array which contains the response data, status code and headers.

> <Array(<FileKey>, Integer, Hash)> dir_assign_with_http_info(opts)

```ruby
begin
  # Assign a file key
  data, status_code, headers = api_instance.dir_assign_with_http_info(opts)
  p status_code # => 2xx
  p headers # => { ... }
  p data # => <FileKey>
rescue OpenapiClient::ApiError => e
  puts "Error when calling DefaultApi->dir_assign_with_http_info: #{e}"
end
```

### Parameters

| Name | Type | Description | Notes |
| ---- | ---- | ----------- | ----- |
| **count** | [**Object**](.md) | how many file ids to assign. Use &lt;fid&gt;_1, &lt;fid&gt;_2 for the assigned additional file ids. e.g. 3,01637037d6_1, 3,01637037d6_2 | [optional] |
| **collection** | [**Object**](.md) | required collection name | [optional] |
| **data_center** | [**Object**](.md) | preferred data center | [optional] |
| **rack** | [**Object**](.md) | preferred rack | [optional] |
| **data_node** | [**Object**](.md) | preferred volume server, e.g. 127.0.0.1:8080 | [optional] |
| **disk** | [**Object**](.md) | If you have disks labelled, this must be supplied to specify the disk type to allocate on. | [optional] |
| **replication** | [**Object**](.md) | replica placement strategy | [optional] |
| **ttl** | [**Object**](.md) | file expiration time limit, example: 3m for 3 minutes. units: m-minute, h-hour, d-day, w-week, M-month, y-year | [optional] |
| **preallocate** | [**Object**](.md) | If no matching volumes, pre-allocate this number of bytes on disk for new volumes. | [optional] |
| **memory_map_max_size_mb** | [**Object**](.md) | Only implemented for windows. Use memory mapped files with specified size for new volumes. | [optional] |
| **writable_volume_count** | [**Object**](.md) | If no matching volumes, create specified number of new volumes. | [optional] |

### Return type

[**FileKey**](FileKey.md)

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json


## dir_lookup

> Object dir_lookup(opts)

Lookup volume

We would need to find out whether the volumes have moved.

### Examples

```ruby
require 'time'
require 'openapi_client'

api_instance = OpenapiClient::DefaultApi.new
opts = {
  volume_id: TODO, # Object | volume id
  collection: TODO, # Object | optionally to speed up the lookup
  file_id: TODO, # Object | If provided, this returns the fileId location and a JWT to update or delete the file.
  read: TODO # Object | works together with \"fileId\", if read=yes, JWT is generated for reads.
}

begin
  # Lookup volume
  result = api_instance.dir_lookup(opts)
  p result
rescue OpenapiClient::ApiError => e
  puts "Error when calling DefaultApi->dir_lookup: #{e}"
end
```

#### Using the dir_lookup_with_http_info variant

This returns an Array which contains the response data, status code and headers.

> <Array(Object, Integer, Hash)> dir_lookup_with_http_info(opts)

```ruby
begin
  # Lookup volume
  data, status_code, headers = api_instance.dir_lookup_with_http_info(opts)
  p status_code # => 2xx
  p headers # => { ... }
  p data # => Object
rescue OpenapiClient::ApiError => e
  puts "Error when calling DefaultApi->dir_lookup_with_http_info: #{e}"
end
```

### Parameters

| Name | Type | Description | Notes |
| ---- | ---- | ----------- | ----- |
| **volume_id** | [**Object**](.md) | volume id | [optional] |
| **collection** | [**Object**](.md) | optionally to speed up the lookup | [optional] |
| **file_id** | [**Object**](.md) | If provided, this returns the fileId location and a JWT to update or delete the file. | [optional] |
| **read** | [**Object**](.md) | works together with \&quot;fileId\&quot;, if read&#x3D;yes, JWT is generated for reads. | [optional] |

### Return type

**Object**

### Authorization

No authorization required

### HTTP request headers

- **Content-Type**: Not defined
- **Accept**: application/json

