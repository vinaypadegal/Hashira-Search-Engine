terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  # Authentication is expected via:
  # - Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN)
  # - Or a shared credentials/profile configured in ~/.aws/credentials
  # If you want to use a specific profile, uncomment and set aws_profile in terraform.tfvars:
  #
  # profile = var.aws_profile
}

data "aws_vpc" "default" {
  default = true
}

data "terraform_remote_state" "kvs_data" {
  backend = "local"

  config = {
    path = "${path.module}/../kvs-data-terraform/terraform.tfstate"
  }
}

data "aws_subnets" "worker_subnets" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }

  filter {
    name   = "availability-zone"
    values = [var.worker_availability_zone]
  }
}

locals {
  # Use explicit subnet_id if provided, otherwise pick the first subnet in the desired AZ
  worker_subnet_id = var.subnet_id != "" ? var.subnet_id : data.aws_subnets.worker_subnets.ids[0]
}

resource "aws_security_group" "ssh" {
  name        = "hashira-ssh"
  description = "Allow SSH from your IP and all traffic between instances in this SG"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "SSH from configured CIDR"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.ssh_allowed_cidr]
  }

  # Allow port 8000 to 10000 from anywhere (no protection)
  ingress {
    description = "Allow TCP 800 from anywhere"
    from_port   = 8000
    to_port     = 10000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
  }

  # Allow HTTPS for frontend/search server
  ingress {
    description = "HTTPS"
    from_port   = 443
    to_port     = 443
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  # Allow HTTP for frontend/search server
  ingress {
    description = "HTTP"
    from_port   = 80
    to_port     = 80
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    ipv6_cidr_blocks = ["::/0"]
  }

  # Allow all traffic between instances that share this security group
  ingress {
    description = "All traffic within this security group"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
}

resource "aws_instance" "kvs_coordinator" {
  ami                         = var.ami_id
  instance_type               = var.instance_type_coord
  key_name                    = var.ssh_key_name
  associate_public_ip_address = true

  # Networking:
  # - Always attach the SSH security group for SSH access.
  # - Optionally attach any extra security groups from var.security_group_ids.
  vpc_security_group_ids = concat([aws_security_group.ssh.id], var.security_group_ids)
  subnet_id              = local.worker_subnet_id

  # Startup script: commands run on first boot of this instance
  # Customize this block with your deployment steps.
  user_data = <<-EOF
    #!/bin/bash
    set -xe

    # Example: basic update and install git
    sudo yum update -y
    sudo yum install -y git
    sudo yum install java-21-amazon-corretto-devel -y


    # install wget
    sudo yum install -y wget
    cd /home/ec2-user

    # # Clean up any previous copies
    # rm -f 555.zip || true

    # # Download and extract
    # wget "${var.download_url}" -O 555.zip
    # unzip -q 555.zip
    
    # # CD into extracted folder (folder name will be provided via variable)
    # cd "${var.project_folder_name}"/


    # mkdir -p bin
    # echo "Compiling source files to bin/..."
    # javac -d bin src/cis5550/**/*.java
    # nohup java -cp bin cis5550.kvs.Coordinator 8000 >/tmp/kvs-coordinator.log 2>&1 &

  EOF

  tags = merge(
    {
      "Name" = "hashira-kvs-coordinator"
    },
    var.extra_tags
  )
}

resource "aws_instance" "kvs_worker" {
  count                       = 10
  ami                         = var.ami_id
  instance_type               = var.instance_type_worker
  key_name                    = var.ssh_key_name
  associate_public_ip_address = true

  volume_tags = merge(
    var.extra_tags,
    {
      Name = "hashira-worker-${count.index}"
    }
  )

  # Networking: same as kvs_coordinator
  vpc_security_group_ids = concat([aws_security_group.ssh.id], var.security_group_ids)
  subnet_id              = local.worker_subnet_id

  user_data = <<-EOF
    #!/bin/bash
    set -xe

    # Example: basic update and install git
    sudo yum update -y
    sudo yum install -y git
    sudo yum install java-21-amazon-corretto-devel -y


    # install wget
    sudo yum install -y wget
    cd /home/ec2-user

    # Clean up any previous copies
    rm -f 555.zip || true

    # Download and extract
    wget "${var.download_url}" -O 555.zip
    unzip -q 555.zip
    
    # CD into extracted folder (folder name will be provided via variable)
    cd "${var.project_folder_name}"/


    mkdir -p bin
    echo "Compiling source files to bin/..."
    javac -d bin src/cis5550/**/*.java

    # KVS worker port (always 8001)
    KVS_PORT=8001
    # Flame worker port: 9001-9006 for first 6 workers
    WORKER_INDEX=${count.index}
    FLAME_PORT=$((9001 + WORKER_INDEX))
    
    # Only start Flame worker on first 6 instances (index 0-5)
    IS_FLAME_WORKER=$([ $WORKER_INDEX -lt 6 ] && echo "true" || echo "false")

    # Prepare persistent data volume (attached as /dev/xvdf)
    DEVICE="/dev/xvdf"
    MOUNT_POINT="/data"

    sudo mkdir -p "$MOUNT_POINT"
    # Wait for the volume to be attached
    sleep 120
    # Check if already mounted
    if mountpoint -q "$MOUNT_POINT"; then
      echo "Volume already mounted at $MOUNT_POINT - DATA PRESERVED"
    else
      # Try mounting first - if it works, filesystem exists
      if sudo mount "$DEVICE" "$MOUNT_POINT" 2>/dev/null; then
        echo "Mounted existing filesystem at $MOUNT_POINT - DATA PRESERVED"
      else
        # Mount failed = no filesystem, so format
        echo "No filesystem found on $DEVICE, formatting as ext4..."
        sudo mkfs -t ext4 -F "$DEVICE"
        sudo mount "$DEVICE" "$MOUNT_POINT"
        echo "Formatted and mounted new filesystem at $MOUNT_POINT"
      fi
    fi

    # Add to /etc/fstab for persistence across reboots
    if ! grep -q "$DEVICE" /etc/fstab; then
      echo "$DEVICE $MOUNT_POINT ext4 defaults,nofail 0 2" | sudo tee -a /etc/fstab
      echo "Added $DEVICE to /etc/fstab for automatic mounting"
    fi

    STORAGE_DIR="$MOUNT_POINT/${var.kvs_data_folder_name}"

    mkdir -p "$STORAGE_DIR"
    
    # # Start KVS Worker on port 8001 (all 10 instances)
    # echo "Starting KVS Worker on port $KVS_PORT with storage at $STORAGE_DIR"
    # nohup java -cp bin cis5550.kvs.Worker $KVS_PORT "$STORAGE_DIR" ${aws_instance.kvs_coordinator.private_ip}:8000 >/tmp/kvs-worker.log 2>&1 &
    
    # # Wait a bit for KVS worker to start
    # sleep 5
    
    # # Start Flame Worker only on first 6 instances (index 0-5)
    # if [ "$IS_FLAME_WORKER" = "true" ]; then
    #   echo "Starting Flame Worker on port $FLAME_PORT"
    #   export FLAME_WORKER_ID="flame-worker-${count.index}"
    #   nohup java -cp bin cis5550.flame.Worker $FLAME_PORT ${aws_instance.flame_coordinator.private_ip}:9000 >/tmp/flame-worker.log 2>&1 &
    # else
    #   echo "Skipping Flame Worker (only first 6 instances run Flame workers)"
    # fi

  EOF

  tags = merge(
    {
      "Name" = "${var.kvs_instance_name}-${count.index}"
    },
    var.extra_tags
  )
}

resource "aws_instance" "flame_coordinator" {
  ami                         = var.ami_id
  instance_type               = var.instance_type_coord
  key_name                    = var.ssh_key_name
  associate_public_ip_address = true

  vpc_security_group_ids = concat([aws_security_group.ssh.id], var.security_group_ids)
  subnet_id              = local.worker_subnet_id

  user_data = <<-EOF
    #!/bin/bash
    set -xe

    sudo yum update -y
    sudo yum install -y git
    sudo yum install -y java-21-amazon-corretto-devel
    sudo yum install -y wget

    cd /home/ec2-user

    sudo dnf install -y java augeas-libs
    sudo python3 -m venv /opt/certbot
    sudo /opt/certbot/bin/pip install --upgrade pip
    sudo /opt/certbot/bin/pip install certbot

    # # Clean up any previous copies
    # rm -f 555.zip || true

    # # Download and extract
    # wget "${var.download_url}" -O 555.zip
    # unzip -q 555.zip
    
    # # CD into extracted folder (folder name will be provided via variable)
    # cd "${var.project_folder_name}"/

    # mkdir -p bin
    # echo "Compiling source files to bin/..."
    # javac -d bin src/cis5550/**/*.java

    # # Start Flame Coordinator on 9000, pointing at KVS coordinator
    # nohup java -cp bin cis5550.flame.Coordinator 9000 ${aws_instance.kvs_coordinator.private_ip}:8000 >/tmp/flame-coordinator.log 2>&1 &

  EOF

  tags = merge(
    {
      "Name" = "hashira-flame-coordinator"
    },
    var.extra_tags
  )
}


resource "aws_volume_attachment" "kvs_data_attach" {
  count       = 10
  device_name = "/dev/xvdf"

  # 1:1 mapping: worker[0] → volume[0], worker[1] → volume[1], etc.
  # Both use count.index (0-based), so indexing matches exactly
  volume_id   = data.terraform_remote_state.kvs_data.outputs.kvs_data_volume_ids[count.index]
  instance_id = aws_instance.kvs_worker[count.index].id
}

