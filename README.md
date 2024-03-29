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
  * Return-Path:
  * Envelope header: From:, MessageID Host, MessageID Domain (helpful to identify original sender)
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

### Usage Options:
```
usage: senderstats [-h] -i <file> [<file> ...] -o <xlsx> [--mfrom MFrom] [--hfrom HFrom] [--rpath RPath] [--msgid MsgID] [--size MsgSz] [--date Date] [--gen-hfrom] [--gen-rpath] [--gen-alignment] [--gen-msgid] [-t THRESHOLD] [--no-display-name] [--remove-prvs] [--decode-srs] [--no-empty-hfrom] [--excluded-domains <domain> [<domain> ...]] [--restrict-domains <domain> [<domain> ...]] [--excluded-senders <sender> [<sender> ...]] [--date-format DateFmt] [--show-skip-detail]

This tool helps identify the top senders based on smart search outbound message exports.

Required arguments (optional):
  -i <file> [<file> ...], --input <file> [<file> ...]  Smart search files to read.
  -o <xlsx>, --output <xlsx>                           Output file

Field mapping arguments (optional):
  --mfrom MFrom                                        CSV field of the envelope sender address. (default=Sender)
  --hfrom HFrom                                        CSV field of the header From: address. (default=Header_From)
  --rpath RPath                                        CSV field of the Return-Path: address. (default=Header_Return-Path)
  --msgid MsgID                                        CSV field of the message ID. (default=Message_ID)
  --size MsgSz                                         CSV field of message size. (default=Message_Size)
  --date Date                                          CSV field of message date/time. (default=Date)

Reporting control arguments (optional):
  --gen-hfrom                                          Generate report showing the header From: data for messages being sent.
  --gen-rpath                                          Generate report showing return path for messages being sent.
  --gen-alignment                                      Generate report showing envelope sender and header From: alignment
  --gen-msgid                                          Generate report showing parsed Message ID. Helps determine the sending system
  -t N, --threshold N                                  Adjust summary report threshold for messages per day to be considered application traffic. (default=100)

Parsing behavior arguments (optional):
  --no-display-name                                    Remove display and use address only. Converts 'Display Name <user@domain.com>' to 'user@domain.com'
  --remove-prvs                                        Remove return path verification strings e.g. prvs=tag=sender@domain.com
  --decode-srs                                         Convert sender rewrite scheme, forwardmailbox+srs=hash=tt=domain.com=user to user@domain.com
  --no-empty-hfrom                                     If the header From: is empty the envelope sender address is used
  --excluded-domains <domain> [<domain> ...]           Exclude domains from processing.
  --restrict-domains <domain> [<domain> ...]           Constrain domains for processing.
  --excluded-senders <sender> [<sender> ...]           Exclude senders from processing.
  --date-format DateFmt                                Date format used to parse the timestamps. (default=%Y-%m-%dT%H:%M:%S.%f%z)

Extended processing controls (optional):
  --show-skip-detail                                   Show skipped details

Usage:
  -h, --help                                           Show this help message and exit
```

### Using the Tool with Proofpoint Smart Search

Export all outbound message traffic as a smart search CSV. You may need to export multiple CSVs if the data per time window exceeds 1M records. The tool can ingest multiple CSVs files at once.

![smart_search_outbound](https://github.com/pfptcommunity/senderstats/assets/83429267/83693152-922e-489a-b06d-a0765ecaf3e8)

Once the files are downlaoded to a target folder, you can run the following command with the path to the files you downloaded and specify a wildard.

```
senderstats -i C:\path\to\downloaded\files\smart_search_results_custer_hosted_2024_03_04_*.csv -o C:\path\to\output\file\my_cluster_hosted.xlsx
```

### Sample Output

The execution results should look similar to the following depending the options you select. 

```
Files to be processed:
C:\Users\ljerabek\Downloads\smart_search_results_cluster_hosted_2024_03_04_173552.csv
C:\Users\ljerabek\Downloads\smart_search_results_cluster_hosted_2024_03_04_173855.csv
C:\Users\ljerabek\Downloads\smart_search_results_cluster_hosted_2024_03_04_173656.csv
C:\Users\ljerabek\Downloads\smart_search_results_cluster_hosted_2024_03_04_173754.csv
C:\Users\ljerabek\Downloads\smart_search_results_cluster_hosted_2024_03_04_173834.csv

Domains excluded from processing:
knowledgefront.com
pphosted.com
ppops.net

Processing:  C:\Users\ljerabek\Downloads\smart_search_results_cluster_hosted_2024_03_04_173552.csv
Processing:  C:\Users\ljerabek\Downloads\smart_search_results_cluster_hosted_2024_03_04_173855.csv
Processing:  C:\Users\ljerabek\Downloads\smart_search_results_cluster_hosted_2024_03_04_173656.csv
Processing:  C:\Users\ljerabek\Downloads\smart_search_results_cluster_hosted_2024_03_04_173754.csv
Processing:  C:\Users\ljerabek\Downloads\smart_search_results_cluster_hosted_2024_03_04_173834.csv

File Processing Summary
Total Records:  4409754
Skipped Records:  2237796

Records by Day
2024-02-03: 43926
2024-02-04: 48567
2024-02-05: 82679
2024-02-06: 100960
2024-02-07: 97990
2024-02-08: 100370
2024-02-09: 85954
2024-02-10: 19740
2024-02-11: 15595
2024-02-12: 94800
2024-02-13: 99043
2024-02-14: 96919
2024-02-15: 95478
2024-02-16: 88463
2024-02-17: 19021
2024-02-18: 16961
2024-02-19: 81489
2024-02-20: 96920
2024-02-21: 103170
2024-02-22: 104562
2024-02-23: 81652
2024-02-24: 17902
2024-02-25: 16311
2024-02-26: 97154
2024-02-27: 99578
2024-02-28: 109633
2024-02-29: 104672
2024-03-01: 117695
2024-03-02: 20002
2024-03-03: 14752

Please see report: C:\Users\ljerabek\Downloads\my_cluster_hosted.xlsx
```

### Sample Summary Statistics

![image](https://github.com/pfptcommunity/senderstats/assets/83429267/93207754-9e58-4e7b-8266-e78eadb48d3a)

### Sample Details (Sender + From by Volume):

![image](https://github.com/pfptcommunity/senderstats/assets/83429267/4fa58247-bf7b-4e9f-ba31-e6173b35da1d)

### Sample Details (Message ID) Inferencing:

![image](https://github.com/pfptcommunity/senderstats/assets/83429267/c6cb1102-c8b5-49c2-b498-51dfa30ae04a)




