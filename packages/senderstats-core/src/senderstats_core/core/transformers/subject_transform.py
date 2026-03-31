from senderstats_core.common.subject_normalizer import normalize_subject
from senderstats_core.data.message_data import MessageData
from senderstats_core.interfaces.transform import Transform


class SubjectTransform(Transform[MessageData, MessageData]):
    def __init__(self):
        super().__init__()

    def transform(self, data: MessageData) -> MessageData:
        snorm, is_resp = normalize_subject(data.subject)
        setattr(data, "subject_norm", snorm)
        setattr(data, "subject_is_response", is_resp)
        return data
