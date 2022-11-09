import json
import logging
import re
from typing import Dict, List

from moto.sns import sns_backends
from moto.sns.exceptions import DuplicateSnsEndpointError
from moto.sns.models import MAXIMUM_MESSAGE_LENGTH
from moto.sns.utils import is_e164  # TODO: validate phone number

from localstack.aws.accounts import get_aws_account_id
from localstack.aws.api import RequestContext
from localstack.aws.api.core import CommonServiceException
from localstack.aws.api.lambda_ import InvocationType
from localstack.aws.api.sns import (
    ActionsList,
    AmazonResourceName,
    BatchEntryIdsNotDistinctException,
    CheckIfPhoneNumberIsOptedOutResponse,
    ConfirmSubscriptionResponse,
    CreateEndpointResponse,
    CreatePlatformApplicationResponse,
    CreateSMSSandboxPhoneNumberResult,
    CreateTopicResponse,
    DelegatesList,
    DeleteSMSSandboxPhoneNumberResult,
    GetEndpointAttributesResponse,
    GetPlatformApplicationAttributesResponse,
    GetSMSAttributesResponse,
    GetSMSSandboxAccountStatusResult,
    GetSubscriptionAttributesResponse,
    GetTopicAttributesResponse,
    InvalidParameterException,
    InvalidParameterValueException,
    LanguageCodeString,
    ListEndpointsByPlatformApplicationResponse,
    ListOriginationNumbersResult,
    ListPhoneNumbersOptedOutResponse,
    ListPlatformApplicationsResponse,
    ListSMSSandboxPhoneNumbersResult,
    ListString,
    ListSubscriptionsByTopicResponse,
    ListSubscriptionsResponse,
    ListTagsForResourceResponse,
    ListTopicsResponse,
    MapStringToString,
    MaxItems,
    MaxItemsListOriginationNumbers,
    MessageAttributeMap,
    NotFoundException,
    OptInPhoneNumberResponse,
    OTPCode,
    PhoneNumber,
    PhoneNumberString,
    PublishBatchRequestEntryList,
    PublishBatchResponse,
    PublishResponse,
    SetSMSAttributesResponse,
    SnsApi,
    String,
    SubscribeResponse,
    SubscriptionAttributesMap,
    TagKeyList,
    TagList,
    TagResourceResponse,
    TooManyEntriesInBatchRequestException,
    TopicAttributesMap,
    UntagResourceResponse,
    VerifySMSSandboxPhoneNumberResult,
    attributeName,
    attributeValue,
    authenticateOnUnsubscribe,
    boolean,
    messageStructure,
    nextToken,
    subscriptionARN,
    topicARN,
    topicName,
)
from localstack.http import Request, Response, Router, route
from localstack.services.edge import ROUTER
from localstack.services.moto import call_moto
from localstack.services.plugins import ServiceLifecycleHook
from localstack.services.sns import constants as sns_constants
from localstack.services.sns.models import SnsMessage, SnsStore, SnsSubscription, sns_stores
from localstack.services.sns.publisher import PublishDispatcher, SnsPublishContext
from localstack.utils.aws import aws_stack
from localstack.utils.strings import short_uid

# set up logger
LOG = logging.getLogger(__name__)


# def publish_message(
#     topic_arn, req_data, headers, subscription_arn=None, skip_checks=False, message_attributes=None
# ):
#     store = SnsProvider.get_store()
#     message = req_data["Message"][0]
#     message_id = str(uuid.uuid4())
#     message_attributes = message_attributes or {}
#
#     target_arn = req_data.get("TargetArn")
#     if target_arn and ":endpoint/" in target_arn:
#         cache = store.platform_endpoint_messages[target_arn] = (
#             store.platform_endpoint_messages.get(target_arn) or []
#         )
#         cache.append(req_data)
#         platform_app, endpoint_attributes = get_attributes_for_application_endpoint(target_arn)
#         message_structure = req_data.get("MessageStructure", [None])[0]
#         LOG.debug("Publishing message to Endpoint: %s | Message: %s", target_arn, message)
#         # TODO: should probably store the delivery logs
#         # https://docs.aws.amazon.com/sns/latest/dg/sns-msg-status.html
#
#         start_thread(
#             lambda _: message_to_endpoint(
#                 target_arn,
#                 message,
#                 message_structure,
#                 endpoint_attributes,
#                 platform_app,
#             ),
#             name="sns-message_to_endpoint",
#         )
#         return message_id
#
#     LOG.debug("Publishing message to TopicArn: %s | Message: %s", topic_arn, message)
#     start_thread(
#         lambda _: message_to_subscribers(
#             message_id,
#             message,
#             topic_arn,
#             # TODO: check
#             req_data,
#             headers,
#             subscription_arn,
#             skip_checks,
#             message_attributes,
#         ),
#         name="sns-message_to_subscribers",
#     )
#
#     return message_id

#
# def get_attributes_for_application_endpoint(target_arn):
#     sns_client = aws_stack.connect_to_service("sns")
#     app_name = target_arn.split("/")[-2]
#
#     endpoint_attributes = None
#     try:
#         endpoint_attributes = sns_client.get_endpoint_attributes(EndpointArn=target_arn)[
#             "Attributes"
#         ]
#     except botocore.exceptions.ClientError:
#         LOG.warning(f"Missing attributes for endpoint: {target_arn}")
#     if not endpoint_attributes:
#         raise CommonServiceException(
#             message="No account found for the given parameters",
#             code="InvalidClientTokenId",
#             status_code=403,
#         )
#
#     platform_apps = sns_client.list_platform_applications()["PlatformApplications"]
#     app = None
#     try:
#         app = [x for x in platform_apps if app_name in x["PlatformApplicationArn"]][0]
#     except IndexError:
#         LOG.warning(f"Missing application: {target_arn}")
#
#     if not app:
#         raise CommonServiceException(
#             message="No account found for the given parameters",
#             code="InvalidClientTokenId",
#             status_code=403,
#         )
#
#     # Validate parameters
#     if "app/GCM/" in app["PlatformApplicationArn"]:
#         validate_gcm_parameters(app, endpoint_attributes)
#
#     return app, endpoint_attributes
#
#
# def message_to_endpoint(target_arn, message, structure, endpoint_attributes, platform_app):
#     if structure == "json":
#         message = json.loads(message)
#
#     platform_name = target_arn.split("/")[-3]
#
#     response = None
#     if platform_name == "GCM":
#         response = send_message_to_GCM(
#             platform_app["Attributes"], endpoint_attributes, message["GCM"]
#         )
#
#     if response is None:
#         LOG.warning("Platform not implemented yet")
#     elif response.status_code != 200:
#         LOG.warning(
#             f"Platform {platform_name} returned response {response.status_code} with content {response.content}"
#         )
#
#
# def validate_gcm_parameters(platform_app: Dict, endpoint_attributes: Dict):
#     server_key = platform_app["Attributes"].get("PlatformCredential", "")
#     if not server_key:
#         raise InvalidParameterException(
#             "Invalid parameter: Attributes Reason: Invalid value for attribute: PlatformCredential: cannot be empty"
#         )
#     headers = {"Authorization": f"key={server_key}", "Content-type": "application/json"}
#     response = requests.post(
#         sns_constants.GCM_URL,
#         headers=headers,
#         data='{"registration_ids":["ABC"]}',
#     )
#
#     if response.status_code == 401:
#         raise InvalidParameterException(
#             "Invalid parameter: Attributes Reason: Platform credentials are invalid"
#         )
#
#     if not endpoint_attributes.get("Token"):
#         raise InvalidParameterException(
#             "Invalid parameter: Attributes Reason: Invalid value for attribute: Token: cannot be empty"
#         )
#
#
# def send_message_to_GCM(app_attributes, endpoint_attributes, message):
#     server_key = app_attributes.get("PlatformCredential", "")
#     token = endpoint_attributes.get("Token", "")
#     data = json.loads(message)
#
#     data["to"] = token
#     headers = {"Authorization": f"key={server_key}", "Content-type": "application/json"}
#
#     response = requests.post(
#         sns_constants.GCM_URL,
#         headers=headers,
#         data=json.dumps(data),
#     )
#     return response


class SnsProvider(SnsApi, ServiceLifecycleHook):
    def __init__(self) -> None:
        super().__init__()
        self._publisher = PublishDispatcher()

    def on_before_stop(self):
        self._publisher.shutdown()

    def on_after_init(self):
        # Allow sent platform endpoint messages to be retrieved from the SNS endpoint
        register_sns_api_resource(ROUTER)

    @staticmethod
    def get_store() -> SnsStore:
        return sns_stores[get_aws_account_id()][aws_stack.get_region()]

    def add_permission(
        self,
        context: RequestContext,
        topic_arn: topicARN,
        label: String,
        aws_account_id: DelegatesList,
        action_name: ActionsList,
    ) -> None:
        call_moto(context)

    def check_if_phone_number_is_opted_out(
        self, context: RequestContext, phone_number: PhoneNumber
    ) -> CheckIfPhoneNumberIsOptedOutResponse:
        moto_response = call_moto(context)
        return CheckIfPhoneNumberIsOptedOutResponse(**moto_response)

    def create_sms_sandbox_phone_number(
        self,
        context: RequestContext,
        phone_number: PhoneNumberString,
        language_code: LanguageCodeString = None,
    ) -> CreateSMSSandboxPhoneNumberResult:
        call_moto(context)
        return CreateSMSSandboxPhoneNumberResult()

    def delete_sms_sandbox_phone_number(
        self, context: RequestContext, phone_number: PhoneNumberString
    ) -> DeleteSMSSandboxPhoneNumberResult:
        call_moto(context)
        return DeleteSMSSandboxPhoneNumberResult()

    def get_endpoint_attributes(
        self, context: RequestContext, endpoint_arn: String
    ) -> GetEndpointAttributesResponse:
        moto_response = call_moto(context)
        return GetEndpointAttributesResponse(**moto_response)

    def get_platform_application_attributes(
        self, context: RequestContext, platform_application_arn: String
    ) -> GetPlatformApplicationAttributesResponse:
        moto_response = call_moto(context)
        return GetPlatformApplicationAttributesResponse(**moto_response)

    def get_sms_attributes(
        self, context: RequestContext, attributes: ListString = None
    ) -> GetSMSAttributesResponse:
        moto_response = call_moto(context)
        return GetSMSAttributesResponse(**moto_response)

    def get_sms_sandbox_account_status(
        self, context: RequestContext
    ) -> GetSMSSandboxAccountStatusResult:
        moto_response = call_moto(context)
        return GetSMSSandboxAccountStatusResult(**moto_response)

    def list_endpoints_by_platform_application(
        self, context: RequestContext, platform_application_arn: String, next_token: String = None
    ) -> ListEndpointsByPlatformApplicationResponse:
        moto_response = call_moto(context)
        return ListEndpointsByPlatformApplicationResponse(**moto_response)

    def list_origination_numbers(
        self,
        context: RequestContext,
        next_token: nextToken = None,
        max_results: MaxItemsListOriginationNumbers = None,
    ) -> ListOriginationNumbersResult:
        moto_response = call_moto(context)
        return ListOriginationNumbersResult(**moto_response)

    def list_phone_numbers_opted_out(
        self, context: RequestContext, next_token: String = None
    ) -> ListPhoneNumbersOptedOutResponse:
        moto_response = call_moto(context)
        return ListPhoneNumbersOptedOutResponse(**moto_response)

    def list_platform_applications(
        self, context: RequestContext, next_token: String = None
    ) -> ListPlatformApplicationsResponse:
        moto_response = call_moto(context)
        return ListPlatformApplicationsResponse(**moto_response)

    def list_sms_sandbox_phone_numbers(
        self, context: RequestContext, next_token: nextToken = None, max_results: MaxItems = None
    ) -> ListSMSSandboxPhoneNumbersResult:
        moto_response = call_moto(context)
        return ListSMSSandboxPhoneNumbersResult(**moto_response)

    def list_subscriptions_by_topic(
        self, context: RequestContext, topic_arn: topicARN, next_token: nextToken = None
    ) -> ListSubscriptionsByTopicResponse:
        moto_response = call_moto(context)
        return ListSubscriptionsByTopicResponse(**moto_response)

    def list_topics(
        self, context: RequestContext, next_token: nextToken = None
    ) -> ListTopicsResponse:
        moto_response = call_moto(context)
        return ListTopicsResponse(**moto_response)

    def opt_in_phone_number(
        self, context: RequestContext, phone_number: PhoneNumber
    ) -> OptInPhoneNumberResponse:
        call_moto(context)
        return OptInPhoneNumberResponse()

    def remove_permission(
        self, context: RequestContext, topic_arn: topicARN, label: String
    ) -> None:
        call_moto(context)

    def set_endpoint_attributes(
        self, context: RequestContext, endpoint_arn: String, attributes: MapStringToString
    ) -> None:
        call_moto(context)

    def set_platform_application_attributes(
        self,
        context: RequestContext,
        platform_application_arn: String,
        attributes: MapStringToString,
    ) -> None:
        call_moto(context)

    def set_sms_attributes(
        self, context: RequestContext, attributes: MapStringToString
    ) -> SetSMSAttributesResponse:
        call_moto(context)
        return SetSMSAttributesResponse()

    def set_topic_attributes(
        self,
        context: RequestContext,
        topic_arn: topicARN,
        attribute_name: attributeName,
        attribute_value: attributeValue = None,
    ) -> None:
        call_moto(context)

    def verify_sms_sandbox_phone_number(
        self, context: RequestContext, phone_number: PhoneNumberString, one_time_password: OTPCode
    ) -> VerifySMSSandboxPhoneNumberResult:
        call_moto(context)
        return VerifySMSSandboxPhoneNumberResult()

    def get_topic_attributes(
        self, context: RequestContext, topic_arn: topicARN
    ) -> GetTopicAttributesResponse:
        moto_response = call_moto(context)
        # todo fix some attributes by moto, see snapshot
        return GetTopicAttributesResponse(**moto_response)

    def publish_batch(
        self,
        context: RequestContext,
        topic_arn: topicARN,
        publish_batch_request_entries: PublishBatchRequestEntryList,
    ) -> PublishBatchResponse:
        if len(publish_batch_request_entries) > 10:
            raise TooManyEntriesInBatchRequestException(
                "The batch request contains more entries than permissible."
            )

        ids = [entry["Id"] for entry in publish_batch_request_entries]
        if len(set(ids)) != len(publish_batch_request_entries):
            raise BatchEntryIdsNotDistinctException(
                "Two or more batch entries in the request have the same Id."
            )

        if topic_arn and ".fifo" in topic_arn:
            if not all(["MessageGroupId" in entry for entry in publish_batch_request_entries]):
                raise InvalidParameterException(
                    "Invalid parameter: The MessageGroupId parameter is required for FIFO topics"
                )
            moto_sns_backend = sns_backends[context.account_id][context.region]
            if moto_sns_backend.get_topic(arn=topic_arn).content_based_deduplication == "false":
                if not all(
                    ["MessageDeduplicationId" in entry for entry in publish_batch_request_entries]
                ):
                    raise InvalidParameterException(
                        "Invalid parameter: The topic should either have ContentBasedDeduplication enabled or MessageDeduplicationId provided explicitly",
                    )

        # TODO: VALIDATE PUBLISH BATCH MESSAGE STRUCTURE!!!!
        store = self.get_store()
        if topic_arn not in store.sns_subscriptions:
            raise NotFoundException(
                "Topic does not exist",
            )

        # TODO: implement SNS MessageDeduplicationId and ContentDeduplication checks
        response = {"Successful": [], "Failed": []}
        for entry in publish_batch_request_entries:
            message_attributes = entry.get("MessageAttributes", {})
            if message_attributes:
                # if a message contains non-valid message attributes
                # will fail for the first non-valid message encountered, and raise ParameterValueInvalid
                validate_message_attributes(message_attributes)

            publish_ctx = SnsPublishContext(
                message=SnsMessage.from_batch_entry(entry),
                store=store,
                request_headers=context.request.headers,
            )

            # TODO: find a scenario where we can fail to send a message synchronously to be able to report it
            # right now, it seems that AWS fails the whole publish if something is wrong in the format of 1 message
            try:
                self._publisher.publish_to_topic(publish_ctx, topic_arn)
                response["Successful"].append(
                    {"Id": entry["Id"], "MessageId": publish_ctx.message.message_id}
                )
            except Exception:
                LOG.exception("Error while batch publishing to %s: entry %s", topic_arn, entry)
                response["Failed"].append({"Id": entry["Id"]})

        return PublishBatchResponse(**response)

    def set_subscription_attributes(
        self,
        context: RequestContext,
        subscription_arn: subscriptionARN,
        attribute_name: attributeName,
        attribute_value: attributeValue = None,
    ) -> None:
        sub = get_subscription_by_arn(subscription_arn)
        if not sub:
            raise NotFoundException("Subscription does not exist")

        # TODO: validate attribute name maybe we can't set anything we want
        sub[attribute_name] = attribute_value
        if attribute_name == "FilterPolicy":
            # TODO: decode and store here?? check created time too
            pass
        elif attribute_name == "RawMessageDelivery":
            # TODO: only for SQS and https(s) subs, + firehose
            pass

    def confirm_subscription(
        self,
        context: RequestContext,
        topic_arn: topicARN,
        token: String,
        authenticate_on_unsubscribe: authenticateOnUnsubscribe = None,
    ) -> ConfirmSubscriptionResponse:
        store = self.get_store()
        sub_arn = None
        # TODO: this is false, we validate only one sub and not all for topic
        for k, v in store.subscription_status.items():
            if v.get("Token") == token and v["TopicArn"] == topic_arn:
                v["Status"] = "Subscribed"
                sub_arn = k
        for k, v in store.sns_subscriptions.items():
            for i in v:
                if i["TopicArn"] == topic_arn:
                    i["PendingConfirmation"] = "false"

        return ConfirmSubscriptionResponse(SubscriptionArn=sub_arn)

    def untag_resource(
        self, context: RequestContext, resource_arn: AmazonResourceName, tag_keys: TagKeyList
    ) -> UntagResourceResponse:
        call_moto(context)
        store = self.get_store()
        store.sns_tags[resource_arn] = [
            t for t in _get_tags(resource_arn) if t["Key"] not in tag_keys
        ]
        return UntagResourceResponse()

    def list_tags_for_resource(
        self, context: RequestContext, resource_arn: AmazonResourceName
    ) -> ListTagsForResourceResponse:
        return ListTagsForResourceResponse(Tags=_get_tags(resource_arn))

    def delete_platform_application(
        self, context: RequestContext, platform_application_arn: String
    ) -> None:
        call_moto(context)

    def delete_endpoint(self, context: RequestContext, endpoint_arn: String) -> None:
        call_moto(context)

    def create_platform_application(
        self, context: RequestContext, name: String, platform: String, attributes: MapStringToString
    ) -> CreatePlatformApplicationResponse:
        # TODO: validate platform
        # see https://docs.aws.amazon.com/cli/latest/reference/sns/create-platform-application.html
        # list of possible values: ADM, Baidu, APNS, APNS_SANDBOX, GCM, MPNS, WNS
        # each platform has a specific way to handle credentials
        # this can also be used for dispatching message to the right platform
        moto_response = call_moto(context)
        return CreatePlatformApplicationResponse(**moto_response)

    def create_platform_endpoint(
        self,
        context: RequestContext,
        platform_application_arn: String,
        token: String,
        custom_user_data: String = None,
        attributes: MapStringToString = None,
    ) -> CreateEndpointResponse:
        # TODO: support mobile app events
        # see https://docs.aws.amazon.com/sns/latest/dg/application-event-notifications.html
        result = None
        try:
            result = call_moto(context)
        except DuplicateSnsEndpointError:
            # TODO: this was unclear in the old provider, check against aws and moto
            moto_sns_backend = sns_backends[context.account_id][context.region]
            for e in moto_sns_backend.platform_endpoints.values():
                if e.token == token:
                    if custom_user_data and custom_user_data != e.custom_user_data:
                        # TODO: check error against aws
                        raise DuplicateSnsEndpointError(
                            f"Endpoint already exist for token: {token} with different attributes"
                        )
        return CreateEndpointResponse(**result)

    def unsubscribe(self, context: RequestContext, subscription_arn: subscriptionARN) -> None:
        call_moto(context)
        store = self.get_store()

        def should_be_kept(current_subscription: SnsSubscription, target_subscription_arn: str):
            if current_subscription["SubscriptionArn"] != target_subscription_arn:
                return True

            if current_subscription["Protocol"] in ["http", "https"]:
                # TODO: actually validate this (re)subscribe behaviour somehow (localhost.run?)
                #  we might need to save the sub token in the store
                subscription_token = short_uid()
                message_ctx = SnsMessage(
                    type="UnsubscribeConfirmation",
                    token=subscription_token,
                    message=f"You have chosen to deactivate subscription {target_subscription_arn}.\nTo cancel this operation and restore the subscription, visit the SubscribeURL included in this message.",
                )
                publish_ctx = SnsPublishContext(
                    message=message_ctx, store=store, request_headers=context.request.headers
                )
                self._publisher.publish_to_topic_subscriber(
                    publish_ctx,
                    topic_arn=current_subscription["TopicArn"],
                    subscription_arn=target_subscription_arn,
                )

            return False

        for topic_arn, existing_subs in store.sns_subscriptions.items():
            store.sns_subscriptions[topic_arn] = [
                sub for sub in existing_subs if should_be_kept(sub, subscription_arn)
            ]

    def get_subscription_attributes(
        self, context: RequestContext, subscription_arn: subscriptionARN
    ) -> GetSubscriptionAttributesResponse:
        sub = get_subscription_by_arn(subscription_arn)
        if not sub:
            raise NotFoundException(f"Subscription with arn {subscription_arn} not found")
        # todo fix some attributes by moto see snapshot
        return GetSubscriptionAttributesResponse(Attributes=sub)

    def list_subscriptions(
        self, context: RequestContext, next_token: nextToken = None
    ) -> ListSubscriptionsResponse:
        moto_response = call_moto(context)
        return ListSubscriptionsResponse(**moto_response)

    def publish(
        self,
        context: RequestContext,
        message: String,
        topic_arn: topicARN = None,
        target_arn: String = None,
        phone_number: String = None,
        subject: String = None,
        message_structure: messageStructure = None,
        message_attributes: MessageAttributeMap = None,
        message_deduplication_id: String = None,
        message_group_id: String = None,
    ) -> PublishResponse:
        # We do not want the request to be forwarded to SNS backend
        if subject == "":
            raise InvalidParameterException("Invalid parameter: Subject")
        if not message or all(not m for m in message):
            raise InvalidParameterException("Invalid parameter: Empty message")

        # TODO: check for topic + target + phone number at the same time?
        if phone_number and not is_e164(phone_number):
            raise InvalidParameterException(
                "Invalid parameter: Phone number does not meet the E164 format"
            )

        if len(message) > MAXIMUM_MESSAGE_LENGTH:
            raise InvalidParameterException("Invalid parameter: Message too long")

        if topic_arn and ".fifo" in topic_arn:
            if not message_group_id:
                raise InvalidParameterException(
                    "Invalid parameter: The MessageGroupId parameter is required for FIFO topics",
                )
            moto_sns_backend = sns_backends[context.account_id][context.region]
            if moto_sns_backend.get_topic(arn=topic_arn).content_based_deduplication == "false":
                if not message_deduplication_id:
                    raise InvalidParameterException(
                        "Invalid parameter: The topic should either have ContentBasedDeduplication enabled or MessageDeduplicationId provided explicitly",
                    )
        elif message_deduplication_id:
            # this is the first one to raise if both are set while the topic is not fifo
            raise InvalidParameterException(
                "Invalid parameter: MessageDeduplicationId Reason: The request includes MessageDeduplicationId parameter that is not valid for this topic type"
            )
        elif message_group_id:
            raise InvalidParameterException(
                "Invalid parameter: MessageGroupId Reason: The request includes MessageGroupId parameter that is not valid for this topic type"
            )

        if message_structure == "json":
            try:
                message = json.loads(message)
                if "default" not in message:
                    raise InvalidParameterException(
                        "Invalid parameter: Message Structure - No default entry in JSON message body"
                    )
            except Exception:
                raise InvalidParameterException(
                    "Invalid parameter: Message Structure - JSON message body failed to parse"
                )

        if message_attributes:
            validate_message_attributes(message_attributes)

        store = self.get_store()

        # No need to create a topic to send SMS or single push notifications with SNS
        # but we can't mock a sending so we only return that it went well
        # TODO: VALIDATE IF TARGET_ARN EXISTS !!!!!
        if not phone_number and not target_arn:
            if topic_arn not in store.sns_subscriptions:
                raise NotFoundException(
                    "Topic does not exist",
                )

        message_ctx = SnsMessage(
            type="Notification",
            message=message,
            message_attributes=message_attributes,
            message_deduplication_id=message_deduplication_id,
            message_group_id=message_group_id,
            message_structure=message_structure,
            subject=subject,
        )
        publish_ctx = SnsPublishContext(
            message=message_ctx, store=store, request_headers=context.request.headers
        )
        self._publisher.publish_to_topic(publish_ctx, topic_arn)

        return PublishResponse(MessageId=message_ctx.message_id)

    def subscribe(
        self,
        context: RequestContext,
        topic_arn: topicARN,
        protocol: String,
        endpoint: String = None,
        attributes: SubscriptionAttributesMap = None,
        return_subscription_arn: boolean = None,
    ) -> SubscribeResponse:
        if not endpoint:
            # TODO: check AWS behaviour (because endpoint is optional)
            raise NotFoundException("Endpoint not specified in subscription")
        if protocol not in sns_constants.SNS_PROTOCOLS:
            raise InvalidParameterException(
                f"Invalid parameter: Amazon SNS does not support this protocol string: {protocol}"
            )
        elif protocol in ["http", "https"] and not endpoint.startswith(f"{protocol}://"):
            raise InvalidParameterException(
                "Invalid parameter: Endpoint must match the specified protocol"
            )
        if ".fifo" in endpoint and ".fifo" not in topic_arn:
            raise InvalidParameterException(
                "Invalid parameter: Invalid parameter: Endpoint Reason: FIFO SQS Queues can not be subscribed to standard SNS topics"
            )
        moto_response = call_moto(context)
        subscription_arn = moto_response.get("SubscriptionArn")
        filter_policy = moto_response.get("FilterPolicy")
        store = self.get_store()
        topic_subs = store.sns_subscriptions[topic_arn] = (
            store.sns_subscriptions.get(topic_arn) or []
        )
        # An endpoint may only be subscribed to a topic once. Subsequent
        # subscribe calls do nothing (subscribe is idempotent).
        for existing_topic_subscription in topic_subs:
            if existing_topic_subscription.get("Endpoint") == endpoint:
                return SubscribeResponse(
                    SubscriptionArn=existing_topic_subscription["SubscriptionArn"]
                )

        subscription = {
            # http://docs.aws.amazon.com/cli/latest/reference/sns/get-subscription-attributes.html
            "TopicArn": topic_arn,
            "Endpoint": endpoint,
            "Protocol": protocol,
            "SubscriptionArn": subscription_arn,
            "FilterPolicy": filter_policy,
            "PendingConfirmation": "true",
        }
        if attributes:
            subscription.update(attributes)
        topic_subs.append(subscription)

        if subscription_arn not in store.subscription_status:
            store.subscription_status[subscription_arn] = {}

        subscription_token = short_uid()
        store.subscription_status[subscription_arn].update(
            {"TopicArn": topic_arn, "Token": subscription_token, "Status": "Not Subscribed"}
        )
        # Send out confirmation message for HTTP(S), fix for https://github.com/localstack/localstack/issues/881
        if protocol in ["http", "https"]:
            message_ctx = SnsMessage(
                type="SubscriptionConfirmation",
                token=subscription_token,
                message=f"You have chosen to subscribe to the topic {topic_arn}.\nTo confirm the subscription, visit the SubscribeURL included in this message.",
            )
            publish_ctx = SnsPublishContext(
                message=message_ctx, store=store, request_headers=context.request.headers
            )
            self._publisher.publish_to_topic_subscriber(
                ctx=publish_ctx,
                topic_arn=topic_arn,
                subscription_arn=subscription_arn,
            )
        elif protocol in ["sqs", "lambda"]:
            # Auto-confirm sqs and lambda subscriptions for now
            # TODO: revisit for multi-account
            self.confirm_subscription(context, topic_arn, subscription_token)
        return SubscribeResponse(SubscriptionArn=subscription_arn)

    def tag_resource(
        self, context: RequestContext, resource_arn: AmazonResourceName, tags: TagList
    ) -> TagResourceResponse:
        # TODO: can this be used to tag any resource when using AWS?
        # each tag key must be unique
        # https://docs.aws.amazon.com/general/latest/gr/aws_tagging.html#tag-best-practices
        unique_tag_keys = {tag["Key"] for tag in tags}
        if len(unique_tag_keys) < len(tags):
            raise InvalidParameterException("Invalid parameter: Duplicated keys are not allowed.")

        call_moto(context)
        store = self.get_store()
        existing_tags = store.sns_tags.get(resource_arn, [])

        def existing_tag_index(item):
            for idx, tag in enumerate(existing_tags):
                if item["Key"] == tag["Key"]:
                    return idx
            return None

        for item in tags:
            existing_index = existing_tag_index(item)
            if existing_index is None:
                existing_tags.append(item)
            else:
                existing_tags[existing_index] = item

        store.sns_tags[resource_arn] = existing_tags
        return TagResourceResponse()

    def delete_topic(self, context: RequestContext, topic_arn: topicARN) -> None:
        call_moto(context)
        store = self.get_store()
        store.sns_subscriptions.pop(topic_arn, None)
        store.sns_tags.pop(topic_arn, None)

    def create_topic(
        self,
        context: RequestContext,
        name: topicName,
        attributes: TopicAttributesMap = None,
        tags: TagList = None,
        data_protection_policy: attributeValue = None,
    ) -> CreateTopicResponse:
        moto_response = call_moto(context)
        store = self.get_store()
        topic_arn = moto_response["TopicArn"]
        tag_resource_success = extract_tags(topic_arn, tags, True, store)
        if not tag_resource_success:
            raise InvalidParameterException(
                "Invalid parameter: Tags Reason: Topic already exists with different tags"
            )
        if tags:
            self.tag_resource(context=context, resource_arn=topic_arn, tags=tags)
        store.sns_subscriptions[topic_arn] = store.sns_subscriptions.get(topic_arn) or []
        return CreateTopicResponse(TopicArn=topic_arn)


def get_subscription_by_arn(sub_arn):
    store = SnsProvider.get_store()
    # TODO maintain separate map instead of traversing all items
    for key, subscriptions in store.sns_subscriptions.items():
        for sub in subscriptions:
            if sub["SubscriptionArn"] == sub_arn:
                return sub


def _get_tags(topic_arn):
    store = SnsProvider.get_store()
    if topic_arn not in store.sns_tags:
        store.sns_tags[topic_arn] = []

    return store.sns_tags[topic_arn]


def is_raw_message_delivery(susbcriber):
    return susbcriber.get("RawMessageDelivery") in ("true", True, "True")


def validate_message_attributes(message_attributes: MessageAttributeMap) -> None:
    """
    Validate the message attributes, and raises an exception if those do not follow AWS validation
    See: https://docs.aws.amazon.com/sns/latest/dg/sns-message-attributes.html
    Regex from: https://stackoverflow.com/questions/40718851/regex-that-does-not-allow-consecutive-dots
    :param message_attributes: the message attributes map for the message
    :raises: InvalidParameterValueException
    :return: None
    """
    for attr_name, attr in message_attributes.items():
        if len(attr_name) > 256:
            raise InvalidParameterValueException(
                "Length of message attribute name must be less than 256 bytes."
            )
        validate_message_attribute_name(attr_name)
        # `DataType` is a required field for MessageAttributeValue
        data_type = attr["DataType"]
        if data_type not in ("String", "Number", "Binary") and not ATTR_TYPE_REGEX.match(data_type):
            raise InvalidParameterValueException(
                f"The message attribute '{attr_name}' has an invalid message attribute type, the set of supported type prefixes is Binary, Number, and String."
            )
        value_key_data_type = "Binary" if data_type.startswith("Binary") else "String"
        value_key = f"{value_key_data_type}Value"
        if value_key not in attr:
            raise InvalidParameterValueException(
                f"The message attribute '{attr_name}' with type '{data_type}' must use field '{value_key_data_type}'."
            )
        elif not attr[value_key]:
            raise InvalidParameterValueException(
                f"The message attribute '{attr_name}' must contain non-empty message attribute value for message attribute type '{data_type}'.",
            )


def validate_message_attribute_name(name: str) -> None:
    """
    Validate the message attribute name with the specification of AWS.
    The message attribute name can contain the following characters: A-Z, a-z, 0-9, underscore(_), hyphen(-), and period (.). The name must not start or end with a period, and it should not have successive periods.
    :param name: message attribute name
    :raises InvalidParameterValueException: if the name does not conform to the spec
    """
    if not sns_constants.MSG_ATTR_NAME_REGEX.match(name):
        # find the proper exception
        if name[0] == ".":
            raise InvalidParameterValueException(
                "Invalid message attribute name starting with character '.' was found."
            )
        elif name[-1] == ".":
            raise InvalidParameterValueException(
                "Invalid message attribute name ending with character '.' was found."
            )

        for idx, char in enumerate(name):
            if char not in sns_constants.VALID_MSG_ATTR_NAME_CHARS:
                # change prefix from 0x to #x, without capitalizing the x
                hex_char = "#x" + hex(ord(char)).upper()[2:]
                raise InvalidParameterValueException(
                    f"Invalid non-alphanumeric character '{hex_char}' was found in the message attribute name. Can only include alphanumeric characters, hyphens, underscores, or dots."
                )
            # even if we go negative index, it will be covered by starting/ending with dot
            if char == "." and name[idx - 1] == ".":
                raise InvalidParameterValueException(
                    "Message attribute name can not have successive '.' character."
                )


def extract_tags(topic_arn, tags, is_create_topic_request, store):
    existing_tags = list(store.sns_tags.get(topic_arn, []))
    existing_sub = store.sns_subscriptions.get(topic_arn, None)
    # if this is none there is nothing to check
    if existing_sub is not None:
        if tags is None:
            tags = []
        for tag in tags:
            # this means topic already created with empty tags and when we try to create it
            # again with other tag value then it should fail according to aws documentation.
            if is_create_topic_request and existing_tags is not None and tag not in existing_tags:
                return False
    return True


def unsubscribe_sqs_queue(queue_url):
    """Called upon deletion of an SQS queue, to remove the queue from subscriptions"""
    # TODO: deprecated, used in legacy SQS provider.
    # delete when legacy is deleted, and behaviour is wrong, this should not happen
    store = SnsProvider.get_store()
    for topic_arn, subscriptions in store.sns_subscriptions.items():
        subscriptions = store.sns_subscriptions.get(topic_arn, [])
        for subscriber in list(subscriptions):
            sub_url = subscriber.get("sqs_queue_url") or subscriber["Endpoint"]
            if queue_url == sub_url:
                subscriptions.remove(subscriber)


def register_sns_api_resource(router: Router):
    """Register the platform endpointmessages retrospection endpoint as an internal LocalStack endpoint."""
    router.add_route_endpoints(SNSServicePlatformEndpointMessagesApiResource())


def _format_platform_endpoint_messages(sent_messages: List[Dict[str, str]]):
    """
    This method format the messages to be more readable and undo the format change that was needed for Moto
    Should be removed once we refactor SNS.
    """
    validated_keys = [
        "TargetArn",
        "TopicArn",
        "Message",
        "MessageAttributes",
        "MessageStructure",
        "Subject",
        "MessageId",
    ]
    formatted_messages = []
    for sent_message in sent_messages:
        msg = {
            key: value[0] if isinstance(value, list) else value
            for key, value in sent_message.items()
            if key in validated_keys
        }
        formatted_messages.append(msg)

    return formatted_messages


class SNSServicePlatformEndpointMessagesApiResource:
    """Provides a REST API for retrospective access to platform endpoint messages sent via SNS.

    This is registered as a LocalStack internal HTTP resource.

    This endpoint accepts:
    - GET param `accountId`: selector for AWS account. If not specified, return fallback `000000000000` test ID
    - GET param `region`: selector for AWS `region`. If not specified, return default "us-east-1"
    - GET param `endpointArn`: filter for `endpointArn` resource in SNS
    - DELETE param `accountId`: selector for AWS account
    - DELETE param `region`: will delete saved messages for `region`
    - DELETE param `endpointArn`: will delete saved messages for `endpointArn`
    """

    @route(sns_constants.PLATFORM_ENDPOINT_MSGS_ENDPOINT, methods=["GET"])
    def on_get(self, request: Request):
        account_id = request.args.get("accountId", get_aws_account_id())
        region = request.args.get("region", "us-east-1")
        filter_endpoint_arn = request.args.get("endpointArn")
        store: SnsStore = sns_stores[account_id][region]
        if filter_endpoint_arn:
            messages = store.platform_endpoint_messages.get(filter_endpoint_arn, [])
            messages = _format_platform_endpoint_messages(messages)
            return {
                "platform_endpoint_messages": {filter_endpoint_arn: messages},
                "region": region,
            }

        platform_endpoint_messages = {
            endpoint_arn: _format_platform_endpoint_messages(messages)
            for endpoint_arn, messages in store.platform_endpoint_messages.items()
        }
        return {
            "platform_endpoint_messages": platform_endpoint_messages,
            "region": region,
        }

    @route(sns_constants.PLATFORM_ENDPOINT_MSGS_ENDPOINT, methods=["DELETE"])
    def on_delete(self, request: Request) -> Response:
        account_id = request.args.get("accountId", get_aws_account_id())
        region = request.args.get("region", "us-east-1")
        filter_endpoint_arn = request.args.get("endpointArn")
        store: SnsStore = sns_stores[account_id][region]
        if filter_endpoint_arn:
            store.platform_endpoint_messages.pop(filter_endpoint_arn, None)
            return Response("", status=204)

        store.platform_endpoint_messages = {}
        return Response("", status=204)
