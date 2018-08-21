#!/usr/bin/env python2
# -*- coding: utf-8 -*-
import boto3

class AmazonApi(object):
    """Amzon API instantiating boto3 module to use Bucket resources"""

    def __init__(self, amazon_key, secret_key, bucket_name):
        self.amazon_key = amazon_key
        self.bucket_name = bucket_name
        self.aws_secret = secret_key
        self.conx = boto3.resource('s3', aws_access_key_id=self.amazon_key, aws_secret_access_key=self.aws_secret)
        self.client = boto3.client('s3', aws_access_key_id=self.amazon_key, aws_secret_access_key=self.aws_secret)

    def create_dir(self, dir_name):
        """Uploads o specific directory in the bucket"""
        creator = self.client.put_object(Bucket=self.bucket_name, Key=dir_name+'/')
        return creator

    def upload_image(self, file_name, file_key, image_folder):
        """Uploads file to specific directory in the bucket"""

        uploader = self.conx.meta.client.upload_file('/tmp/'+file_name, self.bucket_name, image_folder+'/{}'.format(file_key))
        return uploader

    def list_files(self, data_location):
        """Returns all files list from a given location in the bucket"""

        my_bucket = self.conx.Bucket(self.bucket_name) # getting Bucket access
        bucket_list = sorted(my_bucket.objects.filter(Prefix=data_location), key=lambda k: k.last_modified) # getting file list in sorted Order
        files_list = [obj.key.split('/')[1] for obj in bucket_list]
        files_list[:] = [item for item in files_list if item != '']
        self.files_list = files_list
        return self.files_list

    def get_url(self, data_location, **kwargs):
        """Generates URL for all given data in specified Folder"""

        signed_urls = []
        for file_key in list(reversed(self.files_list)):
            file_key = data_location+'/'+file_key
            params = {'Bucket': self.bucket_name, 'Key': file_key}
            params.update(kwargs)
            signed_url = self.client.generate_presigned_url('get_object',
                                                            Params=params,
                                                            ExpiresIn=86400)
            signed_urls.append(signed_url)

        return signed_urls
