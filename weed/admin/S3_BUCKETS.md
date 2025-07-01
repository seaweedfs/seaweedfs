# S3 Bucket Management

The SeaweedFS Admin Interface now includes comprehensive S3 bucket management capabilities.

## Features

### Bucket Overview
- **Dashboard View**: List all S3 buckets with summary statistics
- **Bucket Statistics**: Total buckets, storage usage, object counts
- **Status Monitoring**: Real-time bucket status and health indicators

### Bucket Operations
- **Create Buckets**: Create new S3 buckets
- **Delete Buckets**: Remove buckets and all their contents (with confirmation)
- **View Details**: Browse bucket contents and object listings
- **Export Data**: Export bucket lists to CSV format

### Bucket Information
Each bucket displays:
- **Name**: Bucket identifier
- **Created Date**: When the bucket was created
- **Object Count**: Number of objects stored
- **Total Size**: Storage space used (formatted in KB/MB/GB/TB)
- **Region**: Configured AWS region
- **Status**: Current operational status

## Usage

### Accessing S3 Bucket Management

1. Start the admin server:
   ```bash
   weed admin -port=23646 -masters=localhost:9333 -filer=localhost:8888
   ```

2. Open your browser to: `http://localhost:23646`

3. Click the "S3 Buckets" button in the dashboard toolbar

4. Or navigate directly to: `http://localhost:23646/s3/buckets`

### Creating a New Bucket

1. Click the "Create Bucket" button
2. Enter a valid bucket name (3-63 characters, lowercase letters, numbers, dots, hyphens)
3. Select a region (defaults to us-east-1)
4. Click "Create Bucket"

### Deleting a Bucket

1. Click the trash icon next to the bucket name
2. Confirm the deletion in the modal dialog
3. **Warning**: This permanently deletes the bucket and all its contents

### Viewing Bucket Details

1. Click on a bucket name to view detailed information
2. See all objects within the bucket
3. View object metadata (size, last modified, etc.)

## API Endpoints

The S3 bucket management feature exposes REST API endpoints:

### List Buckets
```
GET /api/s3/buckets
```
Returns JSON array of all buckets with metadata.

### Create Bucket
```
POST /api/s3/buckets
Content-Type: application/json

{
  "name": "my-bucket-name",
  "region": "us-east-1"
}
```

### Delete Bucket
```
DELETE /api/s3/buckets/{bucket-name}
```
Permanently deletes the bucket and all contents.

### Get Bucket Details
```
GET /api/s3/buckets/{bucket-name}
```
Returns detailed bucket information including object listings.

## Technical Implementation

### Backend Integration
- **Filer Integration**: Uses SeaweedFS filer for bucket storage at `/buckets/`
- **Streaming API**: Efficiently handles large bucket listings
- **Error Handling**: Comprehensive error reporting and recovery

### Frontend Features
- **Bootstrap UI**: Modern, responsive web interface
- **Real-time Updates**: Automatic refresh after operations
- **Form Validation**: Client-side bucket name validation
- **Modal Dialogs**: User-friendly create/delete workflows

### Security Considerations
- **Confirmation Dialogs**: Prevent accidental deletions
- **Input Validation**: Prevent invalid bucket names
- **Error Messages**: Clear feedback for failed operations

## Bucket Naming Rules

S3 bucket names must follow these rules:
- 3-63 characters in length
- Contain only lowercase letters, numbers, dots (.), and hyphens (-)
- Start and end with a lowercase letter or number
- Cannot contain spaces or special characters
- Cannot be formatted as an IP address

## Storage Structure

Buckets are stored in the SeaweedFS filer at:
```
/buckets/{bucket-name}/
```

Each bucket directory contains:
- Object files with their original names
- Nested directories for object key prefixes
- Metadata preserved from S3 operations

## Performance Notes

- **Lazy Loading**: Bucket sizes and object counts are calculated on-demand
- **Streaming**: Large bucket listings use streaming responses
- **Caching**: Cluster topology data is cached for performance
- **Pagination**: Large object lists are handled efficiently

## Troubleshooting

### Common Issues

1. **Bucket Creation Fails**
   - Check bucket name follows S3 naming rules
   - Ensure filer is accessible and running
   - Verify sufficient storage space

2. **Bucket Deletion Fails**
   - Ensure bucket exists and is accessible
   - Check for permission issues
   - Verify filer connectivity

3. **Bucket List Empty**
   - Verify filer has `/buckets/` directory
   - Check filer connectivity
   - Ensure buckets were created through S3 API

### Debug Steps

1. Check admin server logs for error messages
2. Verify filer is running and accessible
3. Test filer connectivity: `curl http://localhost:8888/`
4. Check browser console for JavaScript errors

## Future Enhancements

- **Bucket Policies**: Manage access control policies
- **Lifecycle Rules**: Configure object lifecycle management
- **Versioning**: Enable/disable bucket versioning
- **Replication**: Configure cross-region replication
- **Metrics**: Detailed usage and performance metrics
- **Notifications**: Bucket event notifications
- **Search**: Search and filter bucket contents 