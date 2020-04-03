import time
import boto3
import configparser
import json
from botocore.exceptions import ClientError
from cluster_helpers import get_cluster_config, create_clients


def delete_iam_role(iam, role_name):
    try:
        iam.detach_role_policy(RoleName=role_name, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        iam.delete_role(RoleName=role_name) 
    except Exception as e:
        print(e)


def delete_cluster(redshift, cluster_conf):
    try:
        redshift.delete_cluster( ClusterIdentifier=cluster_conf['DWH_CLUSTER_IDENTIFIER'],  SkipFinalClusterSnapshot=True)
    except Exception as e:
        print(e)  
        

def get_cluster_status(redshift, cluster_identifier):
    ret_val = ''
    try:
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
        ret_val = myClusterProps['ClusterStatus'].lower()
    except Exception as e:
        print(e)
    return ret_val

    
def main():
    # 1. Load config
    print("Loading configuration file")
    (aws_conf, cluster_conf, db_conf) = get_cluster_config('dwh.cfg')

    # 2. Create clients
    print("Creating clients")
    (ec2, iam, redshift) = create_clients(aws_conf['AWS_KEY'],
                                     aws_conf['AWS_SECRET'],
                                     cluster_conf['DWH_REGION'])
    
    # 3. Delete IAM role
    print("Deleting IAM role")
    delete_iam_role(iam, cluster_conf['DWH_IAM_ROLE_NAME'])
    
    # 4. Create Cluster and Database
    print("Deleting cluster")
    delete_cluster(redshift, cluster_conf)
    
    # 5. Wait until we're done or timeout
    MAX_ITERATIONS = 30
    current_iteration = 0
    while current_iteration < MAX_ITERATIONS:
        print("Waiting for cluster to be deleted")
        current_iteration += 1
        try:
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=cluster_conf['DWH_CLUSTER_IDENTIFIER'])['Clusters'][0]
            time.sleep(10)
        except ClientError as e:
            if e.response['Error']['Code'] == 'ClusterNotFound':
                # Cluster has been removed   
                print("We're done. Make sure to remove the HOST and ARN config items from your configuratino as they are no longer valid")
                print("To be 100% that no uexpected billing occurs go to the AWS consolse and check that cluster is actually deleted.")
                print("https://console.aws.amazon.com/redshift")
            else:
                print(e)
            return


if __name__ == "__main__":
    main()
