#!/usr/bin/env python
# _*_ coding:utf-8 _*_
from fabric.api import task
import logging
import app_config
import boto3
import botocore
from time import time

logging.basicConfig(format=app_config.LOG_FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(app_config.LOG_LEVEL)


@task
def bulk_acl(bucket_name, acl):
    """
    requires an existing bucket_name and a canned acl: private, public-read ...
    ACL='private'|'public-read'|'public-read-write'|'authenticated-read'|'aws-exec-read'|'bucket-owner-read'|'bucket-owner-full-control',
    """
    AVAILABLE_ACLS = ['private', 'public-read', 'public-read-write',
                      'authenticated-read', 'aws-exec-read',
                      'bucket-owner-read', 'bucket-owner-full-control']
    s3 = boto3.resource('s3')
    # Test bucket existence
    try:
        s3.meta.client.head_bucket(Bucket=bucket_name)
    except botocore.exceptions.ClientError as e:
        # If a client error is thrown, then check that it was a 404 error.
        # If it was a 404 error, then the bucket does not exist.
        error_code = int(e.response['Error']['Code'])
        if error_code == 404:
            logger.error('The bucket:%s was not found' % bucket_name)
            exit()

    if acl not in AVAILABLE_ACLS:
        logger.error('ACL %s not supported' % acl)
        exit()

    bucket = s3.Bucket(bucket_name)
    try:
        start_time = time()
        logger.info("Start time %s" % (start_time))
        # Print out bucket names
        obj_count = 0
        page_num = 0
        for page in bucket.objects.pages():
            page_num += 1
            logger.info('Processing page %s of results' % page_num)
            for obj in page:
                obj_count += 1
                obj.Acl().put(ACL=acl)
                if obj_count % 500 == 0:
                    logger.info('processed %s objects' % obj_count)
        logger.info("--Total Process time: %s seconds" % (time() - start_time))
    except Exception, e:
        logger.error('Exception occured while updating ACL %s' % str(e))
