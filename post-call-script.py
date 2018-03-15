#!/usr/bin/python

##############################################################################
# Trunk Recorder Cloud Utilities version 0.1                                 #
#                                                                            #
# This script should live in the same directory as the trunk-recorder        #
# executable application with +x permissions so that it can be executed by   #
# trunk-recorder. It requires the boto3 module for interaction with AWS, as  #
# well as a functioning install of ffmpeg with aac encoding enabled. It is   #
# only designed to work on *nix based operating systems, and may require     #
# modifications to dpeloy on Windows or OS X                                 #
##############################################################################

import boto3        # For AWS integration
import datetime     # For working with unix epoch timestamps
import csv          # For reading the talkgroup file
import json         # For parsing JSON data
import os.path      # For file system operations
import re           # For regular expressions
# Uncomment the line below if using subprocess (Python 3+)
# import subprocess   # Used to spawn ffmpeg to convert wav to m4a
import sys          # For capturing path to wav file output by trunk-recorder


# Configuration:
CONF = {
        # AWS configuration for boto3 library:
        'aws_access_key_id': '',
        'aws_secret_access_key': '',
        'bucket': '',
        # System name is used for sorting, and does not have to match the
        # name defined in trunk-recorder. This will appear in the URLs
        'system_name': '',
        # Right now, only aac is implemented. Other formats could be added later
        'output_file_format': 'aac',
        # Extension only, no leading dot is expected
        'output_file_extension': 'm4a',
        # Should files be removed from the filesystem after successful upload?
        'delete_wav_file': True,
        'delete_converted_file': True,
        'delete_json_file': False,
        # Path to ffmpeg executable
        'ffmpeg_path': '/usr/bin/ffmpeg',
        # Talkgroup file should match the format used by trunk-recorder and
        # be located in the same directory
        'talkgroup_file': ''
}

# Returns a dictionary of strings containing information from the talkgroup file
# used by trunk-recoder containing the following keys:
#
# talkgroup                 String representation of talkgroup decimal id
# talkgroup_hex             String representation of talkgroup hexadecimal id
# talkgroup_mode            'D' for digital, 'A' for analog, etc.
# talkgroup_name            The "Alpha Tag" or a shorter name for the talkgroup
# talkgroup_description     More specific description of the talkgroup
# talkgroup_type            The "Tag" column from radio reference
# talkgroup_category        General category (e.g. 'Fire', 'Police')
# talkgroup_priority        The trunk-recorder priorty for the talkgroup
def getTalkgroupInfo(tgid):
    output = {}
    with open(CONF['talkgroup_file'], 'rt') as f:
        reader = csv.reader(f)
        try:
            for row in reader:
                if (row[0] == str(tgid)):
                    if len(row) >= 8:
                        output['talkgroup'] = str(tgid)
                        output['talkgroup_hex'] = row[1]
                        output['talkgroup_mode'] = row[2]
                        output['talkgroup_name'] = row[3]
                        output['talkgroup_description'] = row[4]
                        output['talkgroup_type'] = row[5]
                        output['talkgroup_category'] = row[6]
                        output['talkgroup_priority'] = row[7]
                        return output
                    else:
                        print("Invalid talkgroup CSV format")
        except csv.Error as e:
            sys.exit('Error: file %s, line %d: %s' % (f, reader.line_num, e))
    # As a sensible default, if a talkgroup is not found for some reason, return
    # a dictionary that only contains the talkgroup.
    return {'talkgroup': str(tgid)}

# Returns a dictionary with a total description of the call, ready to be
# published or stored in a database. Dictionary keys include:
# talkgroup                 String representation of talkgroup decimal id
# talkgroup_hex             String representation of talkgroup hexadecimal id
# talkgroup_mode            'D' for digital, 'A' for analog, etc.
# talkgroup_name            The "Alpha Tag" or a shorter name for the talkgroup
# talkgroup_description     More specific description of the talkgroup
# talkgroup_type            The "Tag" column from radio reference
# talkgroup_category        General category (e.g. 'Fire', 'Police')
# talkgroup_priority        The trunk-recorder priorty for the talkgroup
# frequency                 The frequency that the call started on
# start_time                Unix timetamp for when the call started
# stop_time                 Unix timestamp for when the call ended
# call_length               The length of the call, in seconds
# emergency                 Emergency call: '0' for no, '1' for yes
# audio_file_path           The path to the compressed audio file
# max_error_count           The highest error count from all transmissions
# max_spike_count           The highest spike count from all transmissions
# units[]                   A list of the units that transmitted on the call
#   unit_id                 The P25 unit identifier of the transmitting unit
#   time                    Unix timestamp when that unit started transmitting
#   audio_file_position     Position in audio file when unit began transmitting
# transmissions[]           A list of frequencies that trunk-recorder captured
#   frequency               The frequency a transmission was captured on
#   time                    The time an individual transmission was captured
#   audio_file_position     Position in audio file when the transmission began
#   length                  The length of the transmission, in seconds
#   error_count             P25 decode error count for the transmission
#   spike_count
def getCallInfo(jsonFilePath, outputPath, talkgroupInfo):
    with open(jsonFilePath, 'rt') as f:
        rawJSON = json.load(f)
    f.close()

    output = talkgroupInfo
    output['frequency'] = rawJSON.get('freq', '')
    output['start_time'] = rawJSON.get('start_time', '')
    output['stop_time'] = rawJSON.get('stop_time', '')
    output['call_length'] = output['stop_time'] - output['start_time']
    output['emergency'] = rawJSON.get('emergency', '')
    output['audio_file_path'] = outputPath

    output['units'] = []
    for source in rawJSON['srcList']:
        unitTransmission = {}
        unitTransmission['unit_id']= source['src']
        unitTransmission['time'] = source['time']
        unitTransmission['audio_file_position'] = source['pos']
        output['units'].append(unitTransmission)

    output['transmissions'] = []
    output['max_error_count'] = 0.000000
    output['max_spike_count'] = 0.000000
    for freq in rawJSON['freqList']:
        transmissionInfo = {}
        transmissionInfo['frequency'] = freq['freq']
        transmissionInfo['time'] = freq['time']
        transmissionInfo['audio_file_position'] = freq['pos']
        transmissionInfo['length'] = freq['len']
        transmissionInfo['error_count'] = freq['error_count']
        transmissionInfo['spike_count'] = freq['spike_count']
        if (transmissionInfo['error_count'] > output['max_error_count']):
            output['max_error_count'] = transmissionInfo['error_count']
        if (transmissionInfo['spike_count'] > output['max_spike_count']):
            output['max_spike_count'] = transmissionInfo['spike_count']
        # Append transmission information to list of transmissions
        output['transmissions'].append(transmissionInfo)

    return talkgroupInfo

# Given a talkgroup id, returns the short name for the talkgroup from the
# same csv file that trunk-recorder uses. If no talkgroup is found, returns
# an empty string
def getTalkgroupName(tgid):
    with open(CONF['talkgroup_file'], 'rt') as f:
        reader = csv.reader(f)
        try:
            for row in reader:
                if (row[0] == str(tgid)):
                    return row[3]
        except csv.Error as e:
            sys.exit('Error: file %s, line %d: %s' % (filename, reader.line_num, e))
    return ''

# Given a path to a wav file on the system, compress that file and return
# a path to the compressed file.
def compressFile(wavFilePath):
    if CONF['output_file_format'] == 'aac':
        wavFileName = os.path.basename(wavFilePath)
        aacFilePath = (
            os.path.splitext(wavFilePath)[0] +
            '.' + CONF['output_file_extension']
        )

        command = (
            'nice -n 19 ' + CONF['ffmpeg_path'] + ' -i ' + wavFilePath +
            ' -c:a aac -strict -2 -b:a 32k -cutoff 18000 ' + aacFilePath +
            ' >/dev/null 2>&1'
        )
        # Execute command (pick one):
        #subprocess.call(command)
        os.system(command)
    # As an example, other formats or even re-naming the wav file could be
    # implemented
    elif CONF['output_file_format'] == 'wav':
        sys.exit('Error: WAV file output not yet implemented')
    else:
        sys.exit('Error: compression format from configuration is invalid')

    return aacFilePath

# Uploads the compressed file to Amazon S3, embedding the json
# description of the call as metadata
def uploadToS3(compressedAudioPath, outputPath, metadata = {}):
    s3 = boto3.client(
        's3',
        aws_access_key_id = CONF['aws_access_key_id'],
        aws_secret_access_key = CONF['aws_secret_access_key']
    )

    try:
        f = open(compressedAudioPath, 'rb')
        response = s3.put_object(
            Bucket=CONF['bucket'],
            Key=outputPath,
            Body=f,
            Metadata=metadata
        )
        f.close()
        return response
    except Exception as e:
        print(e)
        print('Error: there was a problem posting the compressed file to S3')
        raise e

    # If all else fails, return false
    return False

def main():
    # If there is no path to a wav file provided, then exit
    # TODO: Validate path with regex to sanitize user input and prevent
    #       malicious commands from being injected to the system call
    if len(sys.argv) < 2:
        sys.exit(
            'Error: Path to trunk-recorder outputed wav file must be passed ' +
            'to script. Example: %s /path/to/wav_file.wav' % sys.argv[0]
        )

    # Exit script if wav file is not present
    wavFilePath = sys.argv[1]
    if not os.path.exists(wavFilePath):
        sys.exit('Error: %s does not exist on the filesystem.' % wavFilePath)

    # Guess JSON file based on path for wav file and exit if not present
    jsonFilePath = os.path.splitext(wavFilePath)[0] + '.json'
    if not os.path.exists(jsonFilePath):
        sys.exit('Error: %s does not exist on the filesystem.' % wavFilePath)


    # Parse unix epoch time in trunk-recorder generated file to determine
    # talkgroup id, frequency, and unix epoch timestamp
    baseName = os.path.basename(wavFilePath)
    baseNameRegEx = r"(\d+)-(\d+)_([0-9\.]+e\+\d+)(.wav)"

    result = re.search(baseNameRegEx, baseName)
    tgid = result.group(1)
    timestamp = result.group(2)
    freq = result.group(3)

    # Get the talkgroup information from the csv file and format a url-
    # friendly name for S3:
    talkgroupInfo = getTalkgroupInfo(tgid)
    talkgroupName = talkgroupInfo['talkgroup_name']
    if talkgroupName:
        friendlyTalkgroupName = '-' + talkgroupName.replace(' ', '_').lower()
    else:
        friendlyTalkgroupName = ''

    # Start building a path for the output file by parsing the timestamp in
    # the wav file that trunk-recorder generates
    dateTimePath = datetime.datetime.fromtimestamp(
        int(timestamp)
    ).strftime('%Y/%m/%d/%H%M%S')

    # Develop the full output path string:
    # {system}/{YYYY}/{MM}/{DD}/{HHMMSS}-{tgid}[-{talkgroup_name}].{ext}
    outputPath = (
        CONF['system_name'] + '/'
        + dateTimePath + '-'
        # The '-' separating tgid and friendlyTalkgroupName is defined above
        # where the friendlyTalkgroupName is formulated. Talkgroups not listed
        # in the CSV file will not have a name, so the friendlyTalkgroupName
        # will be omitted
        + tgid
        + friendlyTalkgroupName
        + '.' + CONF['output_file_extension']
    )

    # Load call metadata from the JSON file that trunk-recorder ouputs
    callInfo = getCallInfo(jsonFilePath, outputPath, talkgroupInfo)

    # Compress the wav file:
    compressedAudioPath = compressFile(wavFilePath)

    # AWS requires that metadata be a flat string:string format so we have to
    # take the lists from the dictionary and convert them to a JSON string
    metadata = callInfo
    metadata['system'] = CONF['system_name']
    metadata['units'] = json.dumps(metadata['units'])
    metadata['transmissions'] = json.dumps(metadata['transmissions'])
    for value in metadata:
        metadata[value] = str(metadata[value])

    # Upload the file to S3 and print the response
    response = uploadToS3(compressedAudioPath, outputPath, metadata)

    # Clean up local files if S3 upload was successful
    if response['ResponseMetadata']['HTTPStatusCode'] == 200:
        print('Successfully uploaded: ' + compressedAudioPath)
        if CONF['delete_wav_file']:
            os.remove(wavFilePath)
            print('Deleting: ' + wavFilePath)
        if CONF['delete_converted_file']:
            os.remove(compressedAudioPath)
            print('Deleting: ' + compressedAudioPath)
        if CONF['delete_json_file']:
            os.remove(jsonFilePath)
            print('Deleting: ' + jsonFilePath)
    else:
        print('There was an error uploading the file to Amazon S3: \n')
        print(json.dumps(response, indent=4))

if __name__ == "__main__":
    main()
