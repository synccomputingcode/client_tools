# Databricks notebook source
# MAGIC %md
# MAGIC ## Run the next cell alone first to create widgets, then input custom values above.

# COMMAND ----------

# Run this cell first
dbutils.widgets.text("access_key", "", "Access Key ID")
dbutils.widgets.text("secret_key", "", "Secret Access Key")
dbutils.widgets.text("region_name", "us-east-1", "Region")
dbutils.widgets.text("bucket", "", "Bucket")
dbutils.widgets.text("profile_name", "gradient-instance-profile", "Profile Name")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Now click "Run all" above.

# COMMAND ----------

# MAGIC %pip install boto3[crt]
# MAGIC %pip install databricks-sdk --upgrade

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC Get variables from widgets.

# COMMAND ----------

access_key = dbutils.widgets.get('access_key')
secret_key = dbutils.widgets.get('secret_key')
region_name = dbutils.widgets.get('region_name')
bucket = dbutils.widgets.get('bucket')
profile_name = dbutils.widgets.get('profile_name')
list_resource = f"arn:aws:s3:::{bucket}"
write_resource = f"arn:aws:s3:::{bucket}/*"

# COMMAND ----------

# MAGIC %md
# MAGIC Create an AWS session using an Access Key ID and Secret Access Key.
# MAGIC Create an IAM client and an IAM resource.

# COMMAND ----------

import boto3, json

session = boto3.Session(
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name=region_name
)

iam_resource = session.resource('iam')
iam_client = session.client('iam')

# COMMAND ----------

# MAGIC %md
# MAGIC Define the IAM JSON structure for creating the role that will eventually be used in the instance profile, and then create the role.

# COMMAND ----------

# Assume Role JSON
assume_role_doc = {
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "ec2.amazonaws.com"},
        "Action": "sts:AssumeRole"}]
}

# create the role
role_arn = None
try:
    response = iam_resource.create_role(
        RoleName=profile_name, 
        AssumeRolePolicyDocument=json.dumps(assume_role_doc),
    )
    role_arn = response.arn
    print(f"Created role {profile_name}.")
except iam_resource.meta.client.exceptions.EntityAlreadyExistsException as e:
    print(f"Role {profile_name} already exists, nothing to do.")
    for role in iam_resource.roles.all():
        if role.name == profile_name:
            role_arn = role.arn
            break
    if role_arn is None:
        raise Exception(f"Role {profile_name} exists, but could not retrieve ARN.")
print(f"Using Role ARN: {role_arn}")


# COMMAND ----------

# MAGIC %md
# MAGIC Define the IAM JSON structures for creating the policy that will eventually be used in the instance profile, and then create the policy.

# COMMAND ----------

# Instance Policy JSON
instance_policy_doc = {
	"Version": "2012-10-17",
	"Statement": [
		{
			"Effect": "Allow",
			"Action": [
				"ec2:DescribeInstances",
				"ec2:DescribeVolumes"
			],
			"Resource": "*"
		},
		{
			"Effect": "Allow",
			"Action": [
				"s3:ListBucket"
			],
			"Resource": [
				list_resource
			]
		},
		{
			"Effect": "Allow",
			"Action": [
				"s3:PutObject",
				"s3:GetObject",
				"s3:DeleteObject",
				"s3:PutObjectAcl"
			],
			"Resource": [
				write_resource
			]
		},

	]
}

# Create the policy
policy_arn = None
try:
    pol_response = iam_resource.create_policy(
        PolicyName=profile_name, PolicyDocument=json.dumps(instance_policy_doc))
    policy_arn = pol_response.arn
    print(f"Created policy {profile_name}.")
except iam_resource.meta.client.exceptions.EntityAlreadyExistsException as e:
    print(f"Policy {profile_name} already exists, nothing to do.")
    list_pol_response = iam_resource.policies.all()
    for pol in list_pol_response:
        if pol.policy_name == profile_name:
            policy_arn = pol.arn
            break
		if policy_arn is None:
			raise Exception(f"Policy {profile_name} exists, but could not retrieve ARN.")
print(f"Using Policy ARN: {policy_arn}")


# COMMAND ----------

# MAGIC %md
# MAGIC Attach the policy to the new role.

# COMMAND ----------

# If the policy is already attached, there is no error
role = iam_resource.Role(profile_name)
policy_response = role.attach_policy(
    PolicyArn=policy_arn
)

# COMMAND ----------

# MAGIC %md
# MAGIC Create the instance profile.

# COMMAND ----------

# create the instance profile
profile_arn = None
try:
    profile_response = iam_resource.create_instance_profile(
                    InstanceProfileName=profile_name
    )
    profile_arn = profile_response.arn
except iam_resource.meta.client.exceptions.EntityAlreadyExistsException as e:
    print(f"Profile {profile_name} already exists, nothing to do.")
    # capture the ARN if the profile already exists
    for profile in iam_resource.instance_profiles.all():
        if profile.name == profile_name:
            profile_arn = profile.arn
            break
    if profile_arn is None:
        raise Exception(f"Profile {profile_name} exists, but could not retrieve ARN.")
print(f"Using Profile ARN: {profile_arn}.")

# Add the role to the instance profile
try:
    iam_client.add_role_to_instance_profile(
        InstanceProfileName=profile_name, 
        RoleName=profile_name
    )
    print(f"Added role {profile_name} to profile {profile_name}.")
except iam_resource.meta.client.exceptions.LimitExceededException as el:
    print(f"Profile {profile_name} already contains role {profile_name}, nothing to do.")
except iam_resource.meta.client.exceptions.NoSuchEntityException as en:
    raise Exception(f"Role {profile_name} was not created in the cell above. Rerun the notebook.")


# COMMAND ----------

# MAGIC %md
# MAGIC Add the instance profile to Databricks.

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError

# Databricks client
w = WorkspaceClient()

# add the profile to Databricks
try:
    response = w.instance_profiles.add(
        instance_profile_arn=profile_arn,
        skip_validation=True,
        iam_role_arn=role_arn
    )
    print(f"Instance profile {profile_name} added to Databricks.")
except DatabricksError as e:
    if "has already been added to WorkerEnvId" in str(e):
        print(f"Instance profile {profile_name} has already been added to Databricks.")
    else:
        raise e

