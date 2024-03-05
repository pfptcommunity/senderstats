# Proofpoint Sender Analyzer

This tool helps identify the top senders based on smart search outbound message exports or CSV data.

### Requirements:

* Python 3.9+

### Installing the Package

You can install the tool using the following command directly from Github.

```
pip install git+https://github.com/pfptcommunity/senderstats.git
```

or can install the tool using pip.

```
pip install senderstats
```
### Use Cases:
**Outbound message volumes and data transferred by:**
  * Envelope sender
  * Header From:
  * Envelope sender and header From: for SPF alignment purposes

**Summarize message volume information:**
  * Estimated application email traffic based on sender volume threshold:
    * Estimated application data 
    * Estimated application messages 
    * Estimated application average size 
    * Estimated application peak hourly volume
  * Total outbound data
    * Total outbound data 
    * Total outbound messages 
    * Total outbound average size 
    * Total outbound peak hourly volume

### Using the Tool
Export all outbound message traffic as a smart search CSV. You may need to export multiple CSVs if the data per time window exceeds 1M records. The tool can ingest multiple CSVs.

![smart_search_outbound](https://github.com/pfptcommunity/senderstats/assets/83429267/83693152-922e-489a-b06d-a0765ecaf3e8)

Added support for alternate CSV formats by changing the header fields. 

```
usage: senderstats [-h] -i <file> [<file> ...] [--from-field FromField] [--sender-field SenderField] [--msg-size SizeField] [--date-field DateField] [--date-format DateFormat] [--excluded-domains <domain> [<domain> ...]] -o
                   <xlsx> [-t THRESHOLD]

This tool helps identify the top senders based on smart search outbound message exports.

optional arguments:
  -h, --help                                           show this help message and exit
  -i <file> [<file> ...], --input <file> [<file> ...]  Smart search files to read.
  --from-field FromField                               CSV field of the header From: address. (default=Header_From)
  --sender-field SenderField                           CSV field of the From: address. (default=Message_Size)
  --msg-size SizeField                                 CSV field of message size. (default=Message_Size)
  --date-field DateField                               CSV field of message date. (default=Date)
  --date-format DateFormat                             Date format used to parse the timestamps. (default=%Y-%m-%dT%H:%M:%S.%f%z)
  --excluded-domains <domain> [<domain> ...]           Restrict domains to a set of domains.
  -o <xlsx>, --output <xlsx>                           Output file
  -t THRESHOLD, --threshold THRESHOLD                  Integer representing number of messages per day to be considered application traffic. (default=100)
```

![image](https://github.com/pfptcommunity/senderstats/assets/83429267/f79e434e-eb78-4d4c-8e8b-f2d09ffb91a8)

Sample Output:

![image](https://github.com/pfptcommunity/senderstats/assets/83429267/2a8b8f4b-c531-48e1-bdea-b90cc5364559)

Sample Details (Sender + From by Volume):

![image](https://github.com/pfptcommunity/senderstats/assets/83429267/467d2cca-24a8-4373-a92b-90c2b5d3c8b8)



