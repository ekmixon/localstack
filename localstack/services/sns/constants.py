from string import ascii_letters, digits

SNS_PROTOCOLS = [
    "http",
    "https",
    "email",
    "email-json",
    "sms",
    "sqs",
    "application",
    "lambda",
    "firehose",
]

MSG_ATTR_NAME_REGEX = r"^(?!\.)(?!.*\.$)(?!.*\.\.)[a-zA-Z0-9_\-.]+$"
VALID_MSG_ATTR_NAME_CHARS = set(ascii_letters + digits + "." + "-" + "_")

GCM_URL = "https://fcm.googleapis.com/fcm/send"

# Endpoint to access all the PlatformEndpoint sent Messages
PLATFORM_ENDPOINT_MSGS_ENDPOINT = "/_aws/sns/platform-endpoint-messages"
