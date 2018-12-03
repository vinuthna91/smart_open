"""
Script to read a csv file from s3 compatible storage, convert it to avro and write the avro to s3 storage
"""
import os
import avro.io
import avro.datafile
import pandas as pn

import smart_open
import boto
from boto.compat import urlsplit, six
import boto.s3.connection

import logging
import json

access_key_id = access_key_id
secret_access_key = secret_access_key
port=None
hostname=hostname

logging.basicConfig(level=logging.ERROR)

def read_sample_csv(filename_to_read, inConn):
    splitInputDir = urlsplit(filename_to_read, allow_fragments=False)
    if inConn is None:
        inConn = get_objectstore_conn()
    inbucket = inConn.get_bucket(splitInputDir.netloc)
    kr = inbucket.get_key(splitInputDir.path)
    assert kr is not None, 'Unable to read file. File may be absent'
    with smart_open.smart_open(kr, 'r') as fin:
        data = pn.read_csv(fin, header=0, error_bad_lines=False,dtype='str').fillna('NA')
    return data
    
def write_avro_context_manager(data,filename_to_write,inConn=None,num_lines=100):
    avroSchemaOut = gen_schema(data)
    schema = avro.schema.parse(avroSchemaOut)
    dictRes = data.to_dict(orient='records')
    splitInputDir = urlsplit(filename_to_write, allow_fragments=False)
    if inConn is None:
        inConn = get_objectstore_conn()
    inbucket = inConn.get_bucket(splitInputDir.netloc)
    kw = inbucket.get_key(splitInputDir.path,validate=False)
    assert kw is not None, "Unable to get avro key to write"
    with smart_open.smart_open(kw, 'wb') as foutd:
        foutd.flush = lambda: None
        with avro.datafile.DataFileWriter(foutd, avro.io.DatumWriter(), schema) as writer_contextManager:
            for ll, row in enumerate(dictRes):
                writer_contextManager.append(row)
                
def gen_schema(data):
    schema = {
        'type': 'record', 'name': 'data', 'namespace': 'namespace',
        'fields': [
            {'name': field, 'type': ['null', 'string'], 'default': None}
            for field in data.columns
        ]
    }
    return json.dumps(schema, indent=4)
    
def get_objectstore_conn():
    if port is None:
        inConn = boto.connect_s3(
            aws_access_key_id = access_key_id,
            aws_secret_access_key = secret_access_key,
            #port=int(port),
            host = hostname,
            is_secure=False,
            calling_format = boto.s3.connection.OrdinaryCallingFormat(),
            )
    else:
        inConn = boto.connect_s3(
            aws_access_key_id = access_key_id,
            aws_secret_access_key = secret_access_key,
            port=int(port),
            host = hostname,
            is_secure=False,
            calling_format = boto.s3.connection.OrdinaryCallingFormat(),
            )
    return inConn
    
if __name__ == "__main__":
    
    inConn = get_objectstore_conn()
    csvfilename_to_write = 's3a://s3storage-support/sample_csvfile.csv'
    print "reading file",csvfilename_to_write
    data = read_sample_csv(csvfilename_to_write,inConn)
    avrofile_towrite = 's3a://s3storage-support/sample_avro_write.avro'
    print "avrofile to write",avrofile_towrite
    write_avro_context_manager(data,avrofile_towrite,inConn)
                
