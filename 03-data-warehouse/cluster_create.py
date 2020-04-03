import time
import boto3
import configparser
import json
from botocore.exceptions import ClientError
from cluster_helpers import get_cluster_config, create_clients


def create_iam_role(iam, role_name):
    # Create new role
    try:
        dwhRole = iam.create_role(
            Path='/',
            RoleName=role_name,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )
        iam.attach_role_policy(RoleName=role_name,
                               PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                              )['ResponseMetadata']['HTTPStatusCode']

        role_arn = iam.get_role(RoleName=role_name)['Role']['Arn']
        return role_arn
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            print("IAM role already exists")
            role_arn = iam.get_role(RoleName=role_name)['Role']['Arn']
            return role_arn
        else:
            print("Unhandled error occurred while creating IAM role.")
            print(e)
            return None


def create_cluster(redshift, cluster_conf, db_conf, iam_role):
    try:
        response = redshift.create_cluster(        
            #HW
            ClusterType=cluster_conf['DWH_CLUSTER_TYPE'],
            NodeType=cluster_conf['DWH_NODE_TYPE'],
            NumberOfNodes=int(cluster_conf['DWH_NUM_NODES']),

            #Identifiers & Credentials
            DBName=db_conf['DB_NAME'],
            ClusterIdentifier=cluster_conf['DWH_CLUSTER_IDENTIFIER'],
            MasterUsername=db_conf['DB_USER'],
            MasterUserPassword=db_conf['DB_PASSWORD'],

            #Roles (for s3 access)
            IamRoles=[iam_role]  
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'ClusterAlreadyExists':
            # Omit this error, cluester is up and running. We can continue.
            print("Cluster already exists")
        else:
            print("Unhandled error occurred while creating Redshift cluster.")        
            print(e)      
    
        
def get_cluster_status(redshift, cluster_identifier):
    ret_val = ''
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
    if myClusterProps['ClusterStatus'].lower() == 'available':
        ret_val = myClusterProps['Endpoint']['Address']
    return ret_val


def get_cluster_sg(redshift, cluster_identifier):
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
    return myClusterProps['VpcId']


def open_db_ports(ec2, port, redshift, cluster_identifier):
    try:
        sg_id = get_cluster_sg(redshift, cluster_identifier)
        vpc = ec2.Vpc(id=sg_id)
        defaultSg = list(vpc.security_groups.all())[0]
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(port),
            ToPort=int(port)
        )
    except Exception as e:
        if e.response['Error']['Code'] == 'InvalidPermission.Duplicate':
            # Omit this error, port is already open.
            print("Port already open")
        else:
            print("Unhandled error occurred while opening db port.")        
            print(e)  


def main():
    # 1. Load config
    print("Loading configuration file")
    (aws_conf, cluster_conf, db_conf) = get_cluster_config('dwh.cfg')

    # 2. Create clients
    print("Creating clients")
    (ec2, iam, redshift) = create_clients(aws_conf['AWS_KEY'],
                                     aws_conf['AWS_SECRET'],
                                     cluster_conf['DWH_REGION'])
    
    # 3. Create IAM role
    print("Creating IAM role")
    iam_role = create_iam_role(iam, cluster_conf['DWH_IAM_ROLE_NAME'])
    
    # 4. Create Cluster and Database
    print("Creating cluster")
    create_cluster(redshift, cluster_conf, db_conf, iam_role)
    
    # 5. Wait until we're done or timeout
    MAX_ITERATIONS = 30
    dwh_endpoint = ''
    current_iteration = 0
    while len(dwh_endpoint) == 0 and current_iteration < MAX_ITERATIONS:
        print("Waiting for cluster to become available")
        current_iteration += 1
        dwh_endpoint = get_cluster_status(redshift, cluster_conf['DWH_CLUSTER_IDENTIFIER'])
        if len(dwh_endpoint) == 0:
            time.sleep(10)
    
    # 6. Open incoming TCP port for DB connections
    open_db_ports(ec2, db_conf['DB_PORT'], redshift, cluster_conf['DWH_CLUSTER_IDENTIFIER'])

    print("We're done. Make sure to add the below items to your configuration before moving on with the ETL process")
    print(f"HOST={dwh_endpoint}")
    print(f"ARN='{iam_role}'")

if __name__ == "__main__":
    main()
