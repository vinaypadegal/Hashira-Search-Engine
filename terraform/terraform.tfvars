aws_region    = "us-east-1"

# Minimal required settings
ami_id        = "ami-0cae6d6fe6048ca2c"  # Make sure this AMI exists in us-east-1
ssh_key_name  = "amanMBP"                # Existing EC2 key pair name in your AWS account

# OPTIONAL: override default instance type (defaults to t3.micro if omitted)
# instance_type = "t3.micro"

# OPTIONAL networking (for now keep these commented to use your default VPC and security group)
# security_group_ids = ["sg-xxxxxxxx"]
# subnet_id          = "subnet-xxxxxxxx"