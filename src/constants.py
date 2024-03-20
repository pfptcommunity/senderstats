# Define email and domain regex patterns
EMAIL_ADDRESS_REGEX = r'(<?\s*([a-zA-Z0-9.!#$%&’*+\/=?^_`{|}~-]+@([a-z0-9-]+(?:\.[a-z0-9-]+)*)\s*)>?\s*(?:;|$))'
VALID_DOMAIN_REGEX = r"(?!-)[a-z0-9-]{1,63}(?<!-)(\.[a-z]{2,63}){1,2}$"
PRVS_REGEX = r'(ms)?prvs\d?=[^=]*='
SRS_REGEX = r'([^+]*)\+?srs\d{0,2}=[^=]+=[^=]+=([^=]+)=([^@]+)@'

# Email processing exclusions
PROOFPOINT_DOMAIN_EXCLUSIONS = ['ppops.net', 'pphosted.com', 'knowledgefront.com']

# Date formats
DEFAULT_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S.%f%z"

# Thresholds and limits
DEFAULT_THRESHOLD = 100
