from typing import Optional

from models import SnsStore, SnsSubscription


def get_subscription_by_arn(store: SnsStore, sub_arn: str) -> Optional[SnsSubscription]:
    # TODO maintain separate map instead of traversing all items
    for key, subscriptions in store.sns_subscriptions.items():
        for sub in subscriptions:
            if sub["SubscriptionArn"] == sub_arn:
                return sub
