import zlib
from base64 import urlsafe_b64encode as b64e, urlsafe_b64decode as b64d


def unobscure(obscured):
    credential = zlib.decompress(b64d(obscured))
    credential = credential.decode('utf-8')
    # print(credential)
    return credential


if __name__ == '__main__':
    pass