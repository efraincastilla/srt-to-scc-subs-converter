import boto3
import os
import sys
import pycaption
import subprocess
import logging

client = boto3.client('s3')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

tmpPrefix = os.environ['TmpPath']
destPrefix = os.environ['DestPath']

def lambda_handler(event, context):
    for rec in event['Records']:
        s3 = rec['s3']
        fileS3Path = s3['bucket']['name']
        fileS3Key = s3['object']['key']

        # Needed when a delete event on a srt file
        if '.scc' in fileS3Key:
            logger.debug("found .scc in key {0}".format(fileS3Key))
            fileS3Key = fileS3Key.replace('scc', 'srt')

        inputFile = download_srt(fileS3Path, fileS3Key)
        outputFile = os.path.splitext(inputFile)[0] + '.scc'
        convert_srt_2_scc(inputFile, outputFile)
        upload_scc(outputFile, fileS3Path)

    logger.info("{0} records processed.".format(len(event['Records'])))
    return True

def download_srt(file_path, file_key):
    tmp_srt_file = tmpPrefix + "/" + file_key
    directory = os.path.dirname(tmp_srt_file)
    if not os.path.exists(directory):
        os.makedirs(directory)

    print("key=", file_key)
    print("tmp_srt_file=", tmp_srt_file)
    client.download_file(file_path, file_key, tmp_srt_file)
    
    output = subprocess.check_output(["file", tmp_srt_file])
    logger.debug("srt file downloaded to {}".format(str(output, "utf-8")))
    return tmp_srt_file

def convert_srt_2_scc(input_file, output_file):
    
    logger.debug('Start convert_srt_2_scc')
    print('input_file=',input_file)
    print('output_file=',output_file)
    
    try:       
        srt_file = open(input_file, 'rb')
        scc_file = open(output_file, 'w')
        scc_file.write(convert_file(srt_file.read().decode('ascii', 'ignore'), pycaption.SCCWriter()))
        scc_file.close()
        
        print ("End convert_srt_2_scc")
    
    except Exception as e:
            import pdb; pdb.post_mortem(sys.exc_info()[2])
            logging.error('Unable to convert %s: %s', input_file, e)
            srt_file.close()
            scc_file.close()
            raise

def upload_scc(output_file, file_path):
    basename = os.path.basename(output_file)
    full_key = "{0}/{1}".format(destPrefix, basename)
    logger.debug('uploading to S3 bucket: {}, key: {}'.format(file_path, full_key))
    client.upload_file(output_file, file_path, full_key)

def convert_file(input_captions, output_writer):
    print("Start convert_file")
    reader = pycaption.detect_format(input_captions)
    logger.debug(reader)

    if not reader:
        raise RuntimeError('Unrecognized format')

    converter = pycaption.CaptionConverter()
    converter.read(input_captions, reader())
    return converter.write(output_writer)