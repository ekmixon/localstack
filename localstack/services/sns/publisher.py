import ast
import base64
import datetime
import json
import logging
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Dict, List, Union

import requests

from localstack import config
from localstack.aws.api.sns import MessageAttributeMap
from localstack.aws.api.sqs import MessageBodyAttributeMap
from localstack.config import external_service_url
from localstack.services.sns.models import SnsMessage, SnsStore, SnsSubscription
from localstack.utils.aws import aws_stack
from localstack.utils.aws.aws_responses import create_sqs_system_attributes
from localstack.utils.aws.aws_stack import extract_region_from_arn, parse_arn
from localstack.utils.aws.dead_letter_queue import sns_error_to_dead_letter_queue
from localstack.utils.cloudwatch.cloudwatch_util import store_cloudwatch_logs
from localstack.utils.json import json_safe
from localstack.utils.objects import not_none_or
from localstack.utils.strings import long_uid, md5, to_bytes
from localstack.utils.time import timestamp_millis

LOG = logging.getLogger(__name__)

PLATFORM_APPLICATION_REAL = False

# TODO: don't think about platform endpoint yet. This will need to be managed separately, as it's a network call
#  right now, we're not totally sure how AWS manages it: we're having error from account id meaning our tests are
#  just wrong. Focus on other things without the platform, and add it afterwards? hope it doesn't break stuff


# Publish to either a topic which will dispatch to subscribers, or directly to a target (platform endpoint? what
# about SMS?)


@dataclass(frozen=True)
class SnsPublishContext:
    message: SnsMessage
    store: SnsStore
    request_headers: Dict[str, str]


class BaseTopicPublisher:
    def publish(self, context: SnsPublishContext, subscriber: SnsSubscription):
        raise NotImplementedError

    def prepare_message(self, message_context: SnsMessage, subscriber: SnsSubscription) -> str:
        return create_sns_message_body(message_context, subscriber)


class BaseEndpointPublisher:
    def publish(self, context: SnsPublishContext, endpoint: str):
        raise NotImplementedError

    def prepare_message(self, context: SnsPublishContext, endpoint: str) -> str:
        raise NotImplementedError


class LambdaTopicPublisher(BaseTopicPublisher):
    def publish(self, context: SnsPublishContext, subscriber: SnsSubscription):
        try:
            lambda_client = aws_stack.connect_to_service(
                "lambda", region_name=extract_region_from_arn(subscriber["Endpoint"])
            )
            event = self.prepare_message(context.message, subscriber)
            inv_result = lambda_client.invoke(
                FunctionName=subscriber["Endpoint"],
                Payload=to_bytes(json.dumps(event)),
                InvocationType="RequestResponse" if config.SYNCHRONOUS_SNS_EVENTS else "Event",
            )
            status_code = inv_result.get("StatusCode")
            payload = inv_result.get("Payload")

            if payload:
                delivery = {
                    "statusCode": status_code,
                    "providerResponse": payload.read(),
                }
                store_delivery_log(context.message, subscriber, success=True, delivery=delivery)

        except Exception as exc:
            LOG.info(
                "Unable to run Lambda function on SNS message: %s %s", exc, traceback.format_exc()
            )
            store_delivery_log(context.message, subscriber, success=False)
            message_body = create_sns_message_body(
                message_context=context.message, subscriber=subscriber
            )
            sns_error_to_dead_letter_queue(subscriber, message_body, str(exc))

    def prepare_message(self, message_context: SnsMessage, subscriber: SnsSubscription):
        external_url = external_service_url("sns")
        unsubscribe_url = create_unsubscribe_url(external_url, subscriber["SubscriptionArn"])
        # see the format here https://docs.aws.amazon.com/lambda/latest/dg/with-sns.html
        # issue with sdk to serialize the attribute inside lambda
        message_attributes = prepare_message_attributes(message_context.message_attributes)
        # TODO: check lambda parity, might be better to use `create_sns_message_body`
        #  to create the "Sns" field, but beware of json.dumps
        event = {
            "Records": [
                {
                    "EventSource": "aws:sns",
                    "EventVersion": "1.0",
                    "EventSubscriptionArn": subscriber["SubscriptionArn"],
                    "Sns": {
                        "Type": message_context.type or "Notification",
                        "MessageId": message_context.message_id,
                        "TopicArn": subscriber["TopicArn"],
                        "Subject": message_context.subject,
                        "Message": message_context.message_content(subscriber["Protocol"]),
                        "Timestamp": timestamp_millis(),
                        "SignatureVersion": "1",
                        # TODO Add a more sophisticated solution with an actual signature
                        # Hardcoded
                        "Signature": "EXAMPLEpH+..",
                        "SigningCertUrl": "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-0000000000000000000000.pem",
                        "UnsubscribeUrl": unsubscribe_url,
                        "MessageAttributes": message_attributes,
                    },
                }
            ]
        }
        return event


class SqsTopicPublisher(BaseTopicPublisher):
    def publish(self, context: SnsPublishContext, subscriber: SnsSubscription):
        message_context = context.message
        message_body = self.prepare_message(message_context, subscriber)
        sqs_message_attrs = self.create_sqs_message_attributes(
            subscriber, message_context.message_attributes
        )
        try:
            queue_url = aws_stack.sqs_queue_url_for_arn(subscriber["Endpoint"])
            parsed_arn = parse_arn(subscriber["Endpoint"])
            sqs_client = aws_stack.connect_to_service("sqs", region_name=parsed_arn["region"])

            kwargs = {}
            if message_context.message_group_id:
                kwargs["MessageGroupId"] = message_context.message_group_id
            if message_context.message_deduplication_id:
                kwargs["MessageDeduplicationId"] = message_context.message_deduplication_id

            sqs_client.send_message(
                QueueUrl=queue_url,
                MessageBody=message_body,
                MessageAttributes=sqs_message_attrs,
                MessageSystemAttributes=create_sqs_system_attributes(context.request_headers),
                **kwargs,
            )
            store_delivery_log(message_context, subscriber, success=True)
        except Exception as exc:
            LOG.info("Unable to forward SNS message to SQS: %s %s", exc, traceback.format_exc())
            store_delivery_log(message_context, subscriber, success=False)
            sns_error_to_dead_letter_queue(
                subscriber, message_body, str(exc), msg_attrs=sqs_message_attrs
            )
            if "NonExistentQueue" in str(exc):
                LOG.debug("The SQS queue endpoint does not exist anymore")
                # todo: if the queue got deleted, even if we recreate a queue with the same name/url
                #  AWS won't send to it anymore. Would need to unsub/resub.
                #  We should mark this subscription as "broken"

    @staticmethod
    def create_sqs_message_attributes(
        subscriber: SnsSubscription, attributes: MessageAttributeMap
    ) -> MessageBodyAttributeMap:
        message_attributes = {}
        # if RawDelivery is `false`, SNS does not attach SQS message attributes but sends them as part of SNS message
        if not is_raw_message_delivery(subscriber):
            return message_attributes

        for key, value in attributes.items():
            if data_type := value.get("DataType"):
                attribute = {"DataType": data_type}
                if data_type.startswith("Binary"):
                    val = value.get("BinaryValue")
                    attribute["BinaryValue"] = base64.b64decode(to_bytes(val))
                    # base64 decoding might already have happened, in which decode fails.
                    # If decode fails, fallback to whatever is in there.
                    if not attribute["BinaryValue"]:
                        attribute["BinaryValue"] = val

                else:
                    val = value.get("StringValue", "")
                    attribute["StringValue"] = str(val)

                message_attributes[key] = attribute

        return message_attributes


class HttpTopicPublisher(BaseTopicPublisher):
    def publish(self, context: SnsPublishContext, subscriber: SnsSubscription):
        message_context = context.message
        message_body = self.prepare_message(message_context, subscriber)
        try:
            message_headers = {
                "Content-Type": "text/plain",
                # AWS headers according to
                # https://docs.aws.amazon.com/sns/latest/dg/sns-message-and-json-formats.html#http-header
                "x-amz-sns-message-type": message_context.type,
                "x-amz-sns-message-id": message_context.message_id,
                "x-amz-sns-topic-arn": subscriber["TopicArn"],
                "User-Agent": "Amazon Simple Notification Service Agent",
            }
            if message_context.type != "SubscriptionConfirmation":
                # while testing, never had those from AWS but the docs above states it should be there
                message_headers["x-amz-sns-subscription-arn"] = subscriber["SubscriptionArn"]

            # When raw message delivery is enabled, x-amz-sns-rawdelivery needs to be set to 'true'
            # indicating that the message has been published without JSON formatting.
            # https://docs.aws.amazon.com/sns/latest/dg/sns-large-payload-raw-message-delivery.html
            elif message_context.type == "Notification" and is_raw_message_delivery(subscriber):
                message_headers["x-amz-sns-rawdelivery"] = "true"

            response = requests.post(
                subscriber["Endpoint"],
                headers=message_headers,
                data=message_body,
                verify=False,
            )

            delivery = {
                "statusCode": response.status_code,
                "providerResponse": response.content.decode("utf-8"),
            }
            store_delivery_log(message_context, subscriber, success=True, delivery=delivery)

            response.raise_for_status()
        except Exception as exc:
            LOG.info(
                "Received error on sending SNS message, putting to DLQ (if configured): %s", exc
            )
            store_delivery_log(message_context, subscriber, success=False)
            # AWS doesn't send to the DLQ if there's an error trying to deliver a UnsubscribeConfirmation msg
            if message_context.type != "UnsubscribeConfirmation":
                sns_error_to_dead_letter_queue(subscriber, message_body, str(exc))


class EmailJsonTopicPublisher(BaseTopicPublisher):
    def publish(self, context: SnsPublishContext, subscriber: SnsSubscription):
        ses_client = aws_stack.connect_to_service("ses")
        if endpoint := subscriber.get("Endpoint"):
            ses_client.verify_email_address(EmailAddress=endpoint)
            ses_client.verify_email_address(EmailAddress="admin@localstack.com")
            message_body = self.prepare_message(context.message, subscriber)
            ses_client.send_email(
                Source="admin@localstack.com",
                Message={
                    "Body": {"Text": {"Data": message_body}},
                    "Subject": {"Data": "SNS-Subscriber-Endpoint"},
                },
                Destination={"ToAddresses": [endpoint]},
            )
            store_delivery_log(context.message, subscriber, success=True)


class EmailTopicPublisher(EmailJsonTopicPublisher):
    def prepare_message(self, message_context: SnsMessage, subscriber: SnsSubscription):
        return message_context.message_content(subscriber["Protocol"])


class ApplicationTopicPublisher(BaseTopicPublisher):
    def publish(self, context: SnsPublishContext, subscriber: SnsSubscription):
        endpoint_arn = subscriber["Endpoint"]
        message = self.prepare_message(context.message, subscriber)
        cache = context.store.platform_endpoint_messages[endpoint_arn] = (
            context.store.platform_endpoint_messages.get(endpoint_arn) or []
        )
        cache.append(message)

        if PLATFORM_APPLICATION_REAL:
            raise NotImplementedError
            # TODO: rewrite the platform application publishing logic
            #  will need to validate credentials when creating platform app earlier, need thorough testing
            #  will be deactivate though. Feature flag for publishing?
            pass
        store_delivery_log(context.message, subscriber, success=True)

    def prepare_message(
        self, message_context: SnsMessage, subscriber: SnsSubscription
    ) -> Union[str, Dict]:
        if not PLATFORM_APPLICATION_REAL:
            return {
                "TargetArn": subscriber["Endpoint"],
                "TopicArn": subscriber["TopicArn"],
                "SubscriptionArn": subscriber["SubscriptionArn"],
                "Message": message_context.message,  # TODO: depends on platform type!! can get it from subscriber
                "MessageAttributes": message_context.message_attributes,
                "MessageStructure": message_context.message_structure,
                "Subject": message_context.subject,
            }
        else:
            raise NotImplementedError


class SmsTopicPublisher(BaseTopicPublisher):
    def publish(self, context: SnsPublishContext, subscriber: SnsSubscription):
        event = self.prepare_message(context.message, subscriber)
        context.store.sms_messages.append(event)
        LOG.info(
            "Delivering SMS message to %s: %s from topic: %s",
            event["endpoint"],
            event["message_content"],
            event["topic_arn"],
        )

        # MOCK DATA
        delivery = {
            "phoneCarrier": "Mock Carrier",
            "mnc": 270,
            "priceInUSD": 0.00645,
            "smsType": "Transactional",
            "mcc": 310,
            "providerResponse": "Message has been accepted by phone carrier",
            "dwellTimeMsUntilDeviceAck": 200,
        }
        store_delivery_log(context.message, subscriber, success=True, delivery=delivery)

    def prepare_message(self, message_context: SnsMessage, subscriber: SnsSubscription) -> dict:
        return {
            "topic_arn": subscriber["TopicArn"],
            "endpoint": subscriber["Endpoint"],
            "message_content": message_context.message_content(subscriber["Protocol"]),
        }


class FirehoseTopicPublisher(BaseTopicPublisher):
    def publish(self, context: SnsPublishContext, subscriber: SnsSubscription):
        message_body = self.prepare_message(context.message, subscriber)
        try:
            firehose_client = aws_stack.connect_to_service("firehose")
            endpoint = subscriber["Endpoint"]
            if endpoint:
                delivery_stream = aws_stack.extract_resource_from_arn(endpoint).split("/")[1]
                firehose_client.put_record(
                    DeliveryStreamName=delivery_stream, Record={"Data": to_bytes(message_body)}
                )
                store_delivery_log(context.message, subscriber, success=True)
        except Exception as exc:
            LOG.info(
                "Received error on sending SNS message, putting to DLQ (if configured): %s", exc
            )
            # TODO: check delivery log
            # TODO check DLQ?


class SmsPhoneNumberPublisher(BaseEndpointPublisher):
    def publish(self, context: SnsPublishContext, endpoint: str):
        event = self.prepare_message(context.message, endpoint)
        context.store.sms_messages.append(event)
        LOG.info(
            "Delivering SMS message to %s: %s",
            event["endpoint"],
            event["message_content"],
        )

        # TODO: check about delivery logs for individual call
        # hard to know the format

    def prepare_message(self, message_context: SnsMessage, endpoint: str) -> dict:
        return {
            "topic_arn": None,
            "endpoint": endpoint,
            "message_content": message_context.message_content("sms"),
        }


class ApplicationEndpointPublisher(BaseEndpointPublisher):
    def publish(self, context: SnsPublishContext, endpoint: str):
        message = self.prepare_message(context.message, endpoint)
        cache = context.store.platform_endpoint_messages[endpoint] = (
            context.store.platform_endpoint_messages.get(endpoint) or []
        )
        cache.append(message)

        if PLATFORM_APPLICATION_REAL:
            raise NotImplementedError
            # TODO: rewrite the platform application publishing logic
            #  will need to validate credentials when creating platform app earlier, need thorough testing
            #  will be deactivate though. Feature flag for publishing?
            pass
        # TODO: see about delivery log for individual endpoint message
        # store_delivery_log(subscriber, context, success=True)

    def prepare_message(self, message_context: SnsMessage, endpoint: str) -> Union[str, Dict]:
        if not PLATFORM_APPLICATION_REAL:
            return {
                "TargetArn": endpoint,
                "TopicArn": "",
                "SubscriptionArn": "",
                "Message": message_context.message,  # TODO: depends on platform type!! can get it from subscriber/endpoint ARN
                "MessageAttributes": message_context.message_attributes,
                "MessageStructure": message_context.message_structure,
                "Subject": message_context.subject,
                "MessageId": message_context.message_id,
            }
        else:
            raise NotImplementedError


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


def create_sns_message_body(message_context: SnsMessage, subscriber: SnsSubscription) -> str:
    message_type = message_context.type or "Notification"
    protocol = subscriber["Protocol"]
    message_content = message_context.message_content(protocol)

    if message_type == "Notification" and is_raw_message_delivery(subscriber):
        return message_content

    external_url = external_service_url("sns")

    data = {
        "Type": message_type,
        "MessageId": message_context.message_id,
        "TopicArn": subscriber["TopicArn"],
        "Message": message_content,
        "Timestamp": timestamp_millis(),
        "SignatureVersion": "1",
        # TODO Add a more sophisticated solution with an actual signature
        #  check KMS for providing real cert and how to serve them
        # Hardcoded
        "Signature": "EXAMPLEpH+..",
        "SigningCertURL": "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-0000000000000000000000.pem",
    }

    if message_type == "Notification":
        unsubscribe_url = create_unsubscribe_url(external_url, subscriber["SubscriptionArn"])
        data["UnsubscribeURL"] = unsubscribe_url

    elif message_type in ("UnsubscribeConfirmation", "SubscriptionConfirmation"):
        data["Token"] = message_context.token
        data["SubscribeURL"] = create_subscribe_url(
            external_url, subscriber["TopicArn"], message_context.token
        )

    if message_context.subject:
        data["Subject"] = message_context.subject

    if message_context.message_attributes:
        data["MessageAttributes"] = prepare_message_attributes(message_context.message_attributes)

    return json.dumps(data)


def prepare_message_attributes(
    message_attributes: MessageAttributeMap,
) -> Dict[str, Dict[str, str]]:
    attributes = {}
    if not message_attributes:
        return attributes
    # TODO: Number type is not supported for Lambda subscriptions, passed as String
    #  do conversion here
    for attr_name, attr in message_attributes.items():
        data_type = attr["DataType"]
        if data_type.startswith("Binary"):
            # binary payload in base64 encoded by AWS, UTF-8 for JSON
            # https://docs.aws.amazon.com/sns/latest/api/API_MessageAttributeValue.html
            val = base64.b64encode(attr["BinaryValue"]).decode()
        else:
            val = attr.get("StringValue")

        attributes[attr_name] = {
            "Type": data_type,
            "Value": val,
        }
    return attributes


def is_raw_message_delivery(subscriber: SnsSubscription) -> bool:
    return subscriber.get("RawMessageDelivery") in ("true", True, "True")


def store_delivery_log(
    message_context: SnsMessage, subscriber: SnsSubscription, success: bool, delivery: dict = None
):
    log_group_name = subscriber.get("TopicArn", "").replace("arn:aws:", "").replace(":", "/")
    log_stream_name = long_uid()
    invocation_time = int(time.time() * 1000)

    delivery = not_none_or(delivery, {})
    delivery["deliveryId"] = (long_uid(),)
    delivery["destination"] = (subscriber.get("Endpoint", ""),)
    delivery["dwellTimeMs"] = 200
    if not success:
        delivery["attemps"] = 1
    # TODO: check this?? this will fail with Dict? need to save original?? or is it the one from subscriber?
    message = message_context.message
    message_md5 = md5(message) if isinstance(message, str) else md5(json.dumps(message))
    delivery_log = {
        "notification": {
            "messageMD5Sum": message_md5,
            "messageId": message_context.message_id,
            "topicArn": subscriber.get("TopicArn"),
            "timestamp": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f%z"),
        },
        "delivery": delivery,
        "status": "SUCCESS" if success else "FAILURE",
    }

    log_output = json.dumps(json_safe(delivery_log))

    return store_cloudwatch_logs(log_group_name, log_stream_name, log_output, invocation_time)


def create_subscribe_url(external_url, topic_arn, subscription_token):
    return f"{external_url}/?Action=ConfirmSubscription&TopicArn={topic_arn}&Token={subscription_token}"


def create_unsubscribe_url(external_url, subscription_arn):
    return f"{external_url}/?Action=Unsubscribe&SubscriptionArn={subscription_arn}"


class SubscriptionFilter:
    def check_filter_policy_on_message_attributes(self, filter_policy, message_attributes):
        if not filter_policy:
            return True

        for criteria in filter_policy:
            conditions = filter_policy.get(criteria)
            attribute = message_attributes.get(criteria)

            if not self._evaluate_filter_policy_conditions(
                conditions, attribute, message_attributes, criteria
            ):
                return False

        return True

    def _evaluate_filter_policy_conditions(
        self, conditions, attribute, message_attributes, criteria
    ):
        if type(conditions) is not list:
            conditions = [conditions]

        tpe = attribute.get("DataType") or attribute.get("Type") if attribute else None
        val = attribute.get("StringValue") or attribute.get("Value") if attribute else None
        if attribute is not None and tpe == "String.Array":
            values = ast.literal_eval(val)
            for value in values:
                for condition in conditions:
                    if self._evaluate_condition(value, condition, message_attributes, criteria):
                        return True
        else:
            for condition in conditions:
                value = val or None
                if self._evaluate_condition(value, condition, message_attributes, criteria):
                    return True

        return False

    def _evaluate_condition(self, value, condition, message_attributes, criteria):
        if type(condition) is not dict:
            return value == condition
        elif condition.get("exists") is not None:
            return self._evaluate_exists_condition(
                condition.get("exists"), message_attributes, criteria
            )
        elif value is None:
            # the remaining conditions require the value to not be None
            return False
        elif condition.get("anything-but"):
            return value not in condition.get("anything-but")
        elif condition.get("prefix"):
            prefix = condition.get("prefix")
            return value.startswith(prefix)
        elif condition.get("numeric"):
            return self._evaluate_numeric_condition(condition.get("numeric"), value)
        return False

    @staticmethod
    def _is_number(x):
        try:
            float(x)
            return True
        except ValueError:
            return False

    def _evaluate_numeric_condition(self, conditions, value):
        if not self._is_number(value):
            return False

        for i in range(0, len(conditions), 2):
            value = float(value)
            operator = conditions[i]
            operand = float(conditions[i + 1])

            if operator == "=":
                if value != operand:
                    return False
            elif operator == ">":
                if value <= operand:
                    return False
            elif operator == "<":
                if value >= operand:
                    return False
            elif operator == ">=":
                if value < operand:
                    return False
            elif operator == "<=":
                if value > operand:
                    return False

        return True

    @staticmethod
    def _evaluate_exists_condition(conditions, message_attributes, criteria):
        # support for exists: false was added in april 2021
        # https://aws.amazon.com/about-aws/whats-new/2021/04/amazon-sns-grows-the-set-of-message-filtering-operators/
        if conditions:
            return message_attributes.get(criteria) is not None
        else:
            return message_attributes.get(criteria) is None


class PublishDispatcher:
    _http_publisher = HttpTopicPublisher()
    topic_notifiers = {
        "http": _http_publisher,
        "https": _http_publisher,
        "email": EmailTopicPublisher(),
        "email-json": EmailJsonTopicPublisher(),
        "sms": SmsTopicPublisher(),
        "sqs": SqsTopicPublisher(),
        "application": ApplicationTopicPublisher(),
        "lambda": LambdaTopicPublisher(),
        "firehose": FirehoseTopicPublisher(),
    }

    sms_notifier = SmsPhoneNumberPublisher()
    application_notifier = ApplicationEndpointPublisher()
    subscription_filter = SubscriptionFilter()

    def __init__(self, num_thread: int = 5):
        self.executor = ThreadPoolExecutor(num_thread, thread_name_prefix="sns_pub")

    def shutdown(self):
        self.executor.shutdown(wait=False)

    def _should_publish(self, ctx: SnsPublishContext, subscriber: SnsSubscription):
        subscriber_arn = subscriber["SubscriptionArn"]
        filter_policy = ctx.store.subscription_filter_policy.get(subscriber_arn)
        if not filter_policy:
            return True
        # default value is `MessageAttributes`
        match subscriber.get("FilterPolicyScope", "MessageAttributes"):
            case "MessageAttributes":
                return self.subscription_filter.check_filter_policy_on_message_attributes(
                    filter_policy=filter_policy, message_attributes=ctx.message.message_attributes
                )
            case "MessageBody":
                # TODO: not implemented yet
                return True

    def publish_to_topic(self, ctx: SnsPublishContext, topic_arn: str) -> None:
        subscriptions = ctx.store.sns_subscriptions.get(topic_arn, [])
        for subscriber in subscriptions:
            # TODO: check if should send with filter!
            if self._should_publish(ctx, subscriber):
                notifier = self.topic_notifiers[subscriber["Protocol"]]
                LOG.debug("Submitting task to the executor for notifier %s", notifier)
                self.executor.submit(notifier.publish, context=ctx, subscriber=subscriber)

    def publish_to_phone_number(self, ctx: SnsPublishContext, phone_number: str) -> None:
        self.executor.submit(self.sms_notifier.publish, context=ctx, endpoint=phone_number)

    def publish_to_application_endpoint(self, ctx: SnsPublishContext, endpoint_arn: str) -> None:
        self.executor.submit(self.application_notifier.publish, context=ctx, endpoint=endpoint_arn)

    def publish_to_topic_subscriber(
        self, ctx: SnsPublishContext, topic_arn: str, subscription_arn: str
    ) -> None:
        """
        This allows us to publish specific HTTP(S) messages specific to those endpoints, namely
        `SubscriptionConfirmation` and `UnsubscribeConfirmation`. Those are "topic" messages in shape, but are sent
        only to the endpoint subscribing or unsubscribing.
        This only used internally.
        Note: might be needed for multi account SQS and Lambda `SubscriptionConfirmation`
        :param ctx:
        :param topic_arn:
        :param subscription_arn:
        :return: None
        """
        subscriptions: List[SnsSubscription] = ctx.store.sns_subscriptions.get(topic_arn, [])
        for subscriber in subscriptions:
            if subscriber["SubscriptionArn"] == subscription_arn:
                notifier = self.topic_notifiers[subscriber["Protocol"]]
                LOG.debug("Submitting task  to the executor for notifier %s", notifier)
                self.executor.submit(notifier.publish, context=ctx, subscriber=subscriber)
                return
