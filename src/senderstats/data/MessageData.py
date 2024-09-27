class MessageData:
    message_size = 0
    mfrom: str
    hfrom: str
    rpath: str
    rcpts: list
    msgid: str
    msgid_domain: str
    msgid_host: str
    subject: str
    date: str

    def __init__(self):
        self.message_size = 0
        self.mfrom = ''
        self.hfrom = ''
        self.rpath = ''
        self.rcpts = []
        self.msgid = ''
        self.msgid_domain = ''
        self.msgid_host = ''
        self.subject = ''
        self.date = ''
