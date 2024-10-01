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
# When testing on Ubuntu 24.04 the following will not work:
pip install senderstats
```

If you see an error similar to the following:

```
error: externally-managed-environment

× This environment is externally managed
╰─> To install Python packages system-wide, try apt install
    python3-xyz, where xyz is the package you are trying to
    install.

    If you wish to install a non-Debian-packaged Python package,
    create a virtual environment using python3 -m venv path/to/venv.
    Then use path/to/venv/bin/python and path/to/venv/bin/pip. Make
    sure you have python3-full installed.

    If you wish to install a non-Debian packaged Python application,
    it may be easiest to use pipx install xyz, which will manage a
    virtual environment for you. Make sure you have pipx installed.

    See /usr/share/doc/python3.12/README.venv for more information.

note: If you believe this is a mistake, please contact your Python installation or OS distribution provider. You can override this, at the risk of breaking your Python installation or OS, by passing --break-system-packages.
hint: See PEP 668 for the detailed specification.
```

You should use install pipx or you can configure your own virtual environment and use the command referenced above.

```
pipx install senderstats
```

### Use Cases:

**Outbound message volumes and data transferred by:**

* Envelope sender
* Header From:
* Return-Path:
* Envelope header: From:, MessageID Host, MessageID Domain (helpful to identify original sender)
* Envelope sender and header From: for SPF alignment purposes
* Random subject line sampling to help understand the type of traffic
* Peak Hourly Volumes

**Summarize message volume information:**

* Estimated application email traffic based on sender volume threshold:
    * Estimated application data
    * Estimated application messages
    * Estimated application average size
* Total outbound data
    * Total outbound data
    * Total outbound messages
    * Total outbound average size
    * Total outbound peak hourly volume

### Usage Options:

```
usage: senderstats [-h] -i <file> [<file> ...] -o <xlsx> [--mfrom MFrom] [--hfrom HFrom] [--rcpts Rcpts]
                   [--rpath RPath] [--msgid MsgID] [--subject Subject] [--size MsgSz] [--date Date] [--gen-hfrom]
                   [--gen-rpath] [--gen-alignment] [--gen-msgid] [--expand-recipients] [--no-display-name]
                   [--remove-prvs] [--decode-srs] [--no-empty-hfrom] [--sample-subject]
                   [--excluded-domains <domain> [<domain> ...]] [--restrict-domains <domain> [<domain> ...]]
                   [--excluded-senders <sender> [<sender> ...]] [--date-format DateFmt]

This tool helps identify the top senders based on smart search outbound message exports.

Input / Output arguments (required):
  -i <file> [<file> ...], --input <file> [<file> ...]  Smart search files to read.
  -o <xlsx>, --output <xlsx>                           Output file

Field mapping arguments (optional):
  --mfrom MFrom                                        CSV field of the envelope sender address. (default=Sender)
  --hfrom HFrom                                        CSV field of the header From: address. (default=Header_From)
  --rcpts Rcpts                                        CSV field of the header recipient addresses. (default=Recipients)
  --rpath RPath                                        CSV field of the Return-Path: address. (default=Header_Return-Path)
  --msgid MsgID                                        CSV field of the message ID. (default=Message_ID)
  --subject Subject                                    CSV field of the Subject, only used if --sample-subject is specified. (default=Subject)
  --size MsgSz                                         CSV field of message size. (default=Message_Size)
  --date Date                                          CSV field of message date/time. (default=Date)

Reporting control arguments (optional):
  --gen-hfrom                                          Generate report showing the header From: data for messages being sent.
  --gen-rpath                                          Generate report showing return path for messages being sent.
  --gen-alignment                                      Generate report showing envelope sender and header From: alignment
  --gen-msgid                                          Generate report showing parsed Message ID. Helps determine the sending system

Parsing behavior arguments (optional):
  --expand-recipients                                  Expand recipients counts messages by destination. E.g. 1 message going to 3 people, is 3 messages sent.
  --no-display-name                                    Remove display and use address only. Converts 'Display Name <user@domain.com>' to 'user@domain.com'
  --remove-prvs                                        Remove return path verification strings e.g. prvs=tag=sender@domain.com
  --decode-srs                                         Convert sender rewrite scheme, forwardmailbox+srs=hash=tt=domain.com=user to user@domain.com
  --no-empty-hfrom                                     If the header From: is empty the envelope sender address is used
  --sample-subject                                     Enable probabilistic random sampling of subject lines found during processing
  --excluded-domains <domain> [<domain> ...]           Exclude domains from processing.
  --restrict-domains <domain> [<domain> ...]           Constrain domains for processing.
  --excluded-senders <sender> [<sender> ...]           Exclude senders from processing.
  --date-format DateFmt                                Date format used to parse the timestamps. (default=%Y-%m-%dT%H:%M:%S.%f%z)

Usage:
  -h, --help                                           Show this help message and exit
```

### Using the Tool with Proofpoint Smart Search

Export all outbound message traffic as a smart search CSV. You may need to export multiple CSVs if the data per time
window exceeds 1M records. The tool can ingest multiple CSVs files at once.

![smart_search_outbound](https://github.com/pfptcommunity/senderstats/assets/83429267/83693152-922e-489a-b06d-a0765ecaf3e8)

Once the files are downlaoded to a target folder, you can run the following command with the path to the files you
downloaded and specify a wildard.

The following example is the most basic usage:

```
# Windows
senderstats -i C:\path\to\downloaded\files\smart_search_results_cluster_hosted_2024_03_04_*.csv -o C:\path\to\output\file\my_cluster_hosted.xlsx
# Linux
senderstats -i /path/to/downloaded/files/smart_search_results_cluster_hosted_2024_03_04_*.csv -o /path/to/output/file/my_cluster_hosted.xlsx
```

For a more comprehensive report use the following command:

```
# Windows
senderstats -i C:\path\to\downloaded\files\smart_search_results_cluster_hosted_2024_03_04_*.csv -o C:\path\to\output\file\my_cluster_hosted.xlsx --remove-prvs --decode-srs --gen-hfrom --gen-alignment --gen-msgid --sample-subject
# Linux
senderstats -i /path/to/downloaded/files/smart_search_results_cluster_hosted_2024_03_04_*.csv -o /path/to/output/file/my_cluster_hosted.xlsx --remove-prvs --decode-srs --gen-hfrom --gen-alignment --gen-msgid --sample-subject
```

Expanding recipients counts messages by destination via --expand-recipients:

This is useful if you need to determine how many messages were sent to a destination, as a single message can be
addressed to multiple recipients.

```
# Windows
senderstats -i C:\path\to\downloaded\files\smart_search_results_cluster_hosted_2024_03_04_*.csv -o C:\path\to\output\file\my_cluster_hosted.xlsx --remove-prvs --decode-srs --gen-hfrom --gen-alignment --gen-msgid --sample-subject --expand-recipients
# Linux
senderstats -i /path/to/downloaded/files/smart_search_results_cluster_hosted_2024_03_04_*.csv -o /path/to/output/file/my_cluster_hosted.xlsx --remove-prvs --decode-srs --gen-hfrom --gen-alignment --gen-msgid --sample-subject --expand-recipients
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

Please see report: C:\Users\ljerabek\Downloads\my_cluster_hosted.xlsx
```

### Sample Summary Statistics

![image](https://github.com/user-attachments/assets/d30093a1-88de-440e-acc2-06947ccbc559)

### Sample Details (Sender + From by Volume):

![image](https://github.com/pfptcommunity/senderstats/assets/83429267/4fa58247-bf7b-4e9f-ba31-e6173b35da1d)

### Sample Details (Message ID) Inferencing:

![image](https://github.com/pfptcommunity/senderstats/assets/83429267/c6cb1102-c8b5-49c2-b498-51dfa30ae04a)

### Sample Details (Hourly Metrics):

![image](https://github.com/user-attachments/assets/66279533-6582-440f-bf5b-d7496d6fc29a)




