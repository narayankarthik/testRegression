import zlib
import getpass
from base64 import urlsafe_b64encode as b64e, urlsafe_b64decode as b64d


def encode_credentials(data):
    data=bytearray(data, encoding='utf-8')
    return b64e(zlib.compress(data, 9)).decode('utf-8')


if __name__ == '__main__':
    # user_id = input("Enter User ID:")
    # # password = getpass.getpass(prompt="Enter Password:")
    # password = input("Enter Password:")
    # encoded_user_id = encode_credentials(user_id)
    # encoded_password = encode_credentials(password)
    # print("Encoded User ID: {}".format(encoded_user_id))
    # print("Encoded Password: {}".format(encoded_password))
    credential = input("Enter Credential:")
    encoded_credential = encode_credentials(credential)
    print("Encoded Credential:{}".format(encoded_credential))
    # access_kew:+0Rl/fWQ9A/+7I9GeaRS7SNyqVrPa27dIM0KPjjxmvVu8x9RtfO1R4aLFlxdXakquyamFnywp9QA+AStJ7C+0w==
    # encoded_access_kew:eNrTNgjK0U8LD7R01Nc297R0T00MCjYP9qssDCsKSDQyT_H0NfAOyMqqyC0LK7WosAwqSfM3DDJJ9HHLqUiJSMwuLK1MzHXLqywvsAx01HYMLvEyd9Y2KLe1BQDhxRzC
