import boto3
import json
import configparser


def get_cluster_config(filepath):
    aws_validated = True
    cluster_validated = True
    db_validated = True
    aws_conf = None
    cluster_conf = None
    db_conf = None
    
    c = configparser.ConfigParser()
    c.read_file(open(filepath))
    
    if 'AWS_KEY' not in c['AWS'] or len(c['AWS']['AWS_KEY'])==0:
        aws_validated = False
    if 'AWS_SECRET' not in c['AWS'] or len(c['AWS']['AWS_SECRET'])==0:
        aws_validated = False

    if 'DWH_REGION' not in c['CLUSTER'] or len(c['CLUSTER']['DWH_REGION'])==0:
        cluster_validated = False
    if 'DWH_CLUSTER_TYPE' not in c['CLUSTER'] or len(c['CLUSTER']['DWH_CLUSTER_TYPE'])==0:
        cluster_validated = False
    if 'DWH_NUM_NODES' not in c['CLUSTER'] or len(c['CLUSTER']['DWH_NUM_NODES'])==0:
        cluster_validated = False
    if 'DWH_NODE_TYPE' not in c['CLUSTER'] or len(c['CLUSTER']['DWH_NODE_TYPE'])==0:
        cluster_validated = False
    if 'DWH_CLUSTER_IDENTIFIER' not in c['CLUSTER'] or len(c['CLUSTER']['DWH_CLUSTER_IDENTIFIER'])==0:
        cluster_validated = False
    if 'DWH_IAM_ROLE_NAME' not in c['CLUSTER'] or len(c['CLUSTER']['DWH_IAM_ROLE_NAME'])==0:
        cluster_validated = False

    #if 'HOST' not in c['DB'] or len(c['DB']['HOST'])==0:
    #    db_validated = False
    if 'DB_NAME' not in c['DB'] or len(c['DB']['DB_NAME'])==0:
        db_validated = False
    if 'DB_USER' not in c['DB'] or len(c['DB']['DB_USER'])==0:
        db_validated = False
    if 'DB_PASSWORD' not in c['DB'] or len(c['DB']['DB_PASSWORD'])==0:
        db_validated = False
    if 'DB_PORT' not in c['DB'] or len(c['DB']['DB_PORT'])==0:
        db_validated = False        

        
    if aws_validated:
        aws_conf = c['AWS']

    if cluster_validated:
        cluster_conf = c['CLUSTER']

    if db_validated:
        db_conf = c['DB']

    return (aws_conf, cluster_conf, db_conf)


def create_clients(key, secret, region):
    ec2 = boto3.resource('ec2',
                         region_name=region,
                         aws_access_key_id=key,
                         aws_secret_access_key=secret
                        )

#    s3 = boto3.resource('s3',
#                        region_name=region,
#                        aws_access_key_id=key,
#                        aws_secret_access_key=secret
#                       )

    iam = boto3.client('iam',
                        region_name=region,
                        aws_access_key_id=key,
                        aws_secret_access_key=secret
                       )

    redshift = boto3.client('redshift',
                            region_name=region,
                            aws_access_key_id=key,
                            aws_secret_access_key=secret
                           )
    return (ec2, iam, redshift)
