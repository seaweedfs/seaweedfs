# Navigation Menu Test

## Quick Test Guide

To verify that the S3 Buckets link appears in the navigation menu:

### 1. Start the Admin Server
```bash
# Start with minimal setup
weed admin -port=23646 -masters=localhost:9333 -filer=localhost:8888

# Or with dummy values for testing UI only
weed admin -port=23646 -masters=dummy:9333 -filer=dummy:8888
```

### 2. Open Browser
Navigate to: `http://localhost:23646`

### 3. Check Navigation Menu
Look for the sidebar navigation on the left side. You should see:

**CLUSTER Section:**
- Admin
- Cluster  
- Volumes

**MANAGEMENT Section:**
- **S3 Buckets** ← This should be visible!
- File Browser
- Metrics
- Logs

**SYSTEM Section:**
- Configuration
- Maintenance

### 4. Test S3 Buckets Link
- Click on "S3 Buckets" in the sidebar
- Should navigate to `/s3/buckets`
- Should show the S3 bucket management page
- The "S3 Buckets" menu item should be highlighted as active

### 5. Expected Behavior
- Menu item has cube icon: `📦 S3 Buckets`
- Link points to `/s3/buckets`
- Active state highlighting works
- Page loads S3 bucket management interface

## Troubleshooting

If the S3 Buckets link is not visible:

1. **Check Template Generation:**
   ```bash
   cd weed/admin
   templ generate
   ```

2. **Rebuild Binary:**
   ```bash
   cd ../..
   go build -o weed weed/weed.go
   ```

3. **Check Browser Console:**
   - Open Developer Tools (F12)
   - Look for any JavaScript errors
   - Check Network tab for failed requests

4. **Verify File Structure:**
   ```bash
   ls -la weed/admin/view/layout/layout_templ.go
   ```

5. **Check Server Logs:**
   - Look for any error messages when starting admin server
   - Check for template compilation errors

## Files Modified

- `weed/admin/view/layout/layout.templ` - Added S3 Buckets menu item
- `weed/admin/static/js/admin.js` - Updated navigation highlighting
- `weed/command/admin.go` - Added S3 routes

## Expected Navigation Structure

```html
<ul class="nav flex-column">
    <li class="nav-item">
        <a class="nav-link" href="/s3/buckets">
            <i class="fas fa-cube me-2"></i>S3 Buckets
        </a>
    </li>
    <!-- ... other menu items ... -->
</ul>
``` 