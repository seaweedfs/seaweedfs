# SeaweedFS Telemetry Server Deployment

This document describes how to deploy the SeaweedFS telemetry server to a remote server using GitHub Actions, or via Docker.

## Prerequisites

1. A remote Linux server with:
   - SSH access
   - systemd (for service management)
   - Optional: Prometheus and Grafana (for monitoring)

2. GitHub repository secrets configured (see [Setup GitHub Secrets](#setup-github-secrets) below):
   - `TELEMETRY_SSH_PRIVATE_KEY`: SSH private key for accessing the remote server
   - `TELEMETRY_HOST`: Remote server hostname or IP address
   - `TELEMETRY_USER`: Username for SSH access

## Setup GitHub Secrets

Before using the deployment workflow, you need to configure the required secrets in your GitHub repository.

### Step 1: Generate SSH Key Pair

On your local machine, generate a new SSH key pair specifically for deployment:

```bash
# Generate a new SSH key pair
ssh-keygen -t ed25519 -C "seaweedfs-telemetry-deploy" -f ~/.ssh/seaweedfs_telemetry_deploy

# This creates two files:
# ~/.ssh/seaweedfs_telemetry_deploy     (private key)
# ~/.ssh/seaweedfs_telemetry_deploy.pub (public key)
```

### Step 2: Configure Remote Server

Copy the public key to your remote server:

```bash
# Copy public key to remote server
ssh-copy-id -i ~/.ssh/seaweedfs_telemetry_deploy.pub user@your-server.com

# Or manually append to authorized_keys
cat ~/.ssh/seaweedfs_telemetry_deploy.pub | ssh user@your-server.com "mkdir -p ~/.ssh && cat >> ~/.ssh/authorized_keys"
```

Test the SSH connection:

```bash
# Test SSH connection with the new key
ssh -i ~/.ssh/seaweedfs_telemetry_deploy user@your-server.com "echo 'SSH connection successful'"
```

### Step 3: Add Secrets to GitHub Repository

1. Go to your GitHub repository
2. Click on **Settings** tab
3. In the sidebar, click **Secrets and variables** → **Actions**
4. Click **New repository secret** for each of the following:

#### TELEMETRY_SSH_PRIVATE_KEY

```bash
# Display the private key content
cat ~/.ssh/seaweedfs_telemetry_deploy
```

- **Name**: `TELEMETRY_SSH_PRIVATE_KEY`
- **Value**: Copy the entire private key content, including the `-----BEGIN OPENSSH PRIVATE KEY-----` and `-----END OPENSSH PRIVATE KEY-----` lines

#### TELEMETRY_HOST

- **Name**: `TELEMETRY_HOST`
- **Value**: Your server's hostname or IP address (e.g., `telemetry.example.com` or `192.168.1.100`)

#### TELEMETRY_USER

- **Name**: `TELEMETRY_USER`
- **Value**: The username on the remote server (e.g., `ubuntu`, `deploy`, or your username)

### Step 4: Verify Configuration

Create a simple test workflow or manually trigger the deployment to verify the secrets are working correctly.

### Security Best Practices

1. **Dedicated SSH Key**: Use a separate SSH key only for deployment
2. **Limited Permissions**: Create a dedicated user on the remote server with minimal required permissions
3. **Key Rotation**: Regularly rotate SSH keys
4. **Server Access**: Restrict SSH access to specific IP ranges if possible

### Example Server Setup

If you're setting up a new server, here's a basic configuration:

```bash
# On the remote server, create a dedicated user for deployment
sudo useradd -m -s /bin/bash seaweedfs-deploy
sudo usermod -aG sudo seaweedfs-deploy  # Only if sudo access is needed

# Switch to the deployment user
sudo su - seaweedfs-deploy

# Create SSH directory
mkdir -p ~/.ssh
chmod 700 ~/.ssh

# Add your public key (paste the content of seaweedfs_telemetry_deploy.pub)
nano ~/.ssh/authorized_keys
chmod 600 ~/.ssh/authorized_keys
```

### Troubleshooting

#### SSH Connection Issues

```bash
# Test SSH connection manually
ssh -i ~/.ssh/seaweedfs_telemetry_deploy -v user@your-server.com

# Check SSH key permissions
ls -la ~/.ssh/seaweedfs_telemetry_deploy*
# Should show: -rw------- for private key, -rw-r--r-- for public key
```

#### GitHub Actions Fails

1. **Check secrets**: Ensure all three secrets are properly set in GitHub
2. **Verify SSH key**: Make sure the entire private key (including headers/footers) is copied
3. **Test connectivity**: Manually SSH to the server from your local machine
4. **Check user permissions**: Ensure the remote user has necessary permissions

## GitHub Actions Workflow

The deployment workflow (`.github/workflows/deploy_telemetry.yml`) provides two main operations:

### 1. First-time Setup

Run this once to set up the remote server:

1. Go to GitHub Actions in your repository
2. Select "Deploy Telemetry Server" workflow
3. Click "Run workflow"
4. Check "Run first-time server setup"
5. Click "Run workflow"

This will:
- Create necessary directories on the remote server
- Set up systemd service configuration
- Configure log rotation
- Upload Grafana dashboard and Prometheus configuration
- Enable the telemetry service (but not start it yet)

**Note**: The setup only prepares the infrastructure. You need to run a deployment afterward to install and start the telemetry server.


### 2. Deploy Updates

To deploy updates, manually trigger deployment:
1. Go to GitHub Actions in your repository
2. Select "Deploy Telemetry Server" workflow
3. Click "Run workflow"
4. Check "Deploy telemetry server to remote server"
5. Click "Run workflow"

## Docker Deployment

You can build and run the telemetry server using Docker locally or on a remote host.

### Build

- Using Docker Compose (recommended):

```bash
docker compose -f telemetry/docker-compose.yml build telemetry-server
```

- Using docker build directly (from the repository root):

```bash
docker build -t seaweedfs-telemetry \
  -f telemetry/server/Dockerfile \
  .
```

### Run

- With Docker Compose:

```bash
docker compose -f telemetry/docker-compose.yml up -d telemetry-server
```

- With docker run:

```bash
docker run -d --name telemetry-server \
  -p 8080:8080 \
  seaweedfs-telemetry
```

Notes:

- The container runs as a non-root user by default.
- The image listens on port `8080` inside the container. Map it with `-p <host_port>:8080`.
- You can pass flags to the server by appending them after the image name, e.g. `docker run -d -p 8353:8080 seaweedfs-telemetry -port=8353 -dashboard=false`.

## Server Directory Structure

After setup, the remote server will have:

```
~/seaweedfs-telemetry/
├── bin/
│   └── telemetry-server          # Binary executable
├── logs/
│   ├── telemetry.log            # Application logs
│   └── telemetry.error.log      # Error logs
├── data/                        # Data directory (if needed)
├── grafana-dashboard.json       # Grafana dashboard configuration
└── prometheus.yml               # Prometheus configuration
```

## Service Management

The telemetry server runs as a systemd service:

```bash
# Check service status
sudo systemctl status telemetry.service

# View logs
sudo journalctl -u telemetry.service -f

# Restart service
sudo systemctl restart telemetry.service

# Stop/start service
sudo systemctl stop telemetry.service
sudo systemctl start telemetry.service
```

## Accessing the Service

After deployment, the telemetry server will be available at (default ports shown; adjust if you override with `-port`):

- Docker default: `8080`
  - **Dashboard**: `http://your-server:8080`
  - **API**: `http://your-server:8080/api/*`
  - **Metrics**: `http://your-server:8080/metrics`
  - **Health Check**: `http://your-server:8080/health`

- Systemd example (if you configured a different port, e.g. `8353`):
  - **Dashboard**: `http://your-server:8353`
  - **API**: `http://your-server:8353/api/*`
  - **Metrics**: `http://your-server:8353/metrics`
  - **Health Check**: `http://your-server:8353/health`

## Optional: Prometheus and Grafana Integration

### Prometheus Setup

1. Install Prometheus on your server
2. Update `/etc/prometheus/prometheus.yml` to include:
   ```yaml
   scrape_configs:
     - job_name: 'seaweedfs-telemetry'
       static_configs:
         - targets: ['localhost:8353']
       metrics_path: '/metrics'
   ```

### Grafana Setup

1. Install Grafana on your server
2. Import the dashboard from `~/seaweedfs-telemetry/grafana-dashboard.json`
3. Configure Prometheus as a data source pointing to your Prometheus instance

## Troubleshooting

### Deployment Fails

1. Check GitHub Actions logs for detailed error messages
2. Verify SSH connectivity: `ssh user@host`
3. Ensure all required secrets are configured in GitHub

### Service Won't Start

1. Check service logs: `sudo journalctl -u telemetry.service`
2. Verify binary permissions: `ls -la ~/seaweedfs-telemetry/bin/`
3. Test binary manually: `~/seaweedfs-telemetry/bin/telemetry-server -help`

### Port Conflicts

If port 8353 is already in use:

1. Edit the systemd service: `sudo systemctl edit telemetry.service`
2. Add override configuration:
   ```ini
   [Service]
   ExecStart=
   ExecStart=/home/user/seaweedfs-telemetry/bin/telemetry-server -port=8354
   ```
3. Reload and restart: `sudo systemctl daemon-reload && sudo systemctl restart telemetry.service`

## Security Considerations

1. **Firewall**: Consider restricting access to telemetry ports
2. **SSH Keys**: Use dedicated SSH keys with minimal permissions
3. **User Permissions**: Run the service as a non-privileged user
4. **Network**: Consider running on internal networks only

## Monitoring

Monitor the deployment and service health:

- **GitHub Actions**: Check workflow runs for deployment status
- **System Logs**: `sudo journalctl -u telemetry.service`
- **Application Logs**: `tail -f ~/seaweedfs-telemetry/logs/telemetry.log`
- **Health Endpoint**: `curl http://localhost:8353/health`
- **Metrics**: `curl http://localhost:8353/metrics` 