"""
This module contains utility methods to connect to GCP resources and make updates.
"""
#pylint: disable=E0611
#pylint: disable=E1101
#pylint: disable=E0401
from google.cloud import bigquery
from google.cloud import storage
from google.cloud import pubsub_v1
from google.protobuf import field_mask_pb2
from google.cloud import bigtable_admin_v2
from google.cloud import compute_v1
from googleapiclient import discovery


#pylint: disable=W0603
BQ_CLIENT = None
GCS_CLIENT = None
PUBLISHSER_CLIENT = None
SUBSCRIBER_CLIENT = None
BIGTABLE_ADMIN_CLIENT = None
COMPUTE_CLIENT = None

def get_bq_client():
    """
      Returns BigQuery client
    """
    global BQ_CLIENT
    if BQ_CLIENT is None:
        BQ_CLIENT = bigquery.Client()
    return BQ_CLIENT

def get_gcs_client():
    """
      Returns GCS client
    """
    global GCS_CLIENT
    if GCS_CLIENT is None:
        GCS_CLIENT = storage.Client()
    return GCS_CLIENT

def get_publisher_client():
    """
      Returns Pub/sub publisher client
    """
    global PUBLISHSER_CLIENT
    if PUBLISHSER_CLIENT is None:
        PUBLISHSER_CLIENT = pubsub_v1.PublisherClient()
    return PUBLISHSER_CLIENT

def get_subscriber_client():
    """
      Returns Pub/sub subscriber client
    """
    global SUBSCRIBER_CLIENT
    if SUBSCRIBER_CLIENT is None:
        SUBSCRIBER_CLIENT = pubsub_v1.SubscriberClient()
    return SUBSCRIBER_CLIENT

def get_bigtable_admin_client():
    """
      Returns BigTable admin client
    """
    global BIGTABLE_ADMIN_CLIENT
    if BIGTABLE_ADMIN_CLIENT is None:
        BIGTABLE_ADMIN_CLIENT = bigtable_admin_v2.BigtableInstanceAdminClient()
    return BIGTABLE_ADMIN_CLIENT

def get_compute_client():
    """
      Returns GCE client
    """
    global COMPUTE_CLIENT
    if COMPUTE_CLIENT is None:
        COMPUTE_CLIENT = compute_v1.InstancesClient()
    return COMPUTE_CLIENT


def merge_labels(existing_dict, new_dict):
    """
    Utility to merge existing and new labels
    """
    for k,v in new_dict.items():
        existing_dict[k] = v
    return existing_dict


def get_existing_bq_dataset_labels(dataset_id):
    """
    Get existing dataset labels
    """
    client = get_bq_client()
    dataset = client.get_dataset(dataset_id)  # Make an API request.
    return dataset.labels


def update_bq_dataset_labels(dataset_id, labels):
    """
    Update dataset labels
    """
    client = get_bq_client()
    dataset = client.get_dataset(dataset_id)  # Make an API request.
    dataset.labels = labels
    dataset = client.update_dataset(dataset, ["labels"])  # Make an API request.


def get_existing_gcs_labels(bucket):
    """
    Get existing GCS labels
    """
    bucket = get_gcs_client().get_bucket(bucket)
    return bucket.labels


def update_gcs_labels(bucket, labels):
    """
    Update GCS labels
    """
    bucket = get_gcs_client().get_bucket(bucket)
    bucket.labels = labels
    bucket.patch()


def get_existing_topic_labels(project_id, topic_id):
    """
    Get existing labels
    """
    publisher = get_publisher_client()
    topic_path = publisher.topic_path(project_id, topic_id)
    topic = publisher.get_topic(request={"topic": topic_path})
    return topic.labels


def remove_existing_topic_labels(project_id, topic_id):
    """
    Remove existing topic labels
    """
    publisher = get_publisher_client()
    publisher_path = publisher.topic_path(project_id, topic_id)
    # Get the current topic configuration
    topic = publisher.get_topic(request={"topic": publisher_path})
    # Update the labels
    topic.labels = {}
    # Update the topic on the server
    update_mask = field_mask_pb2.FieldMask(paths=["labels"])
    publisher.update_topic(request={"topic": topic, "update_mask": update_mask})


def update_topic_labels(project_id, topic_id, labels):
    """
    Update topic labels
    """
    publisher = get_publisher_client()
    topic_path = publisher.topic_path(project_id, topic_id)
    # Get the current topic configuration
    topic = publisher.get_topic(request={"topic": topic_path})

    # Update the labels
    topic.labels.clear()
    topic.labels.update(labels)

    # Update the topic on the server
    update_mask = field_mask_pb2.FieldMask(paths=["labels"])
    publisher.update_topic(request={"topic": topic, "update_mask": update_mask})


def get_existing_subscription_labels(project_id, subscription_id):
    """
    Get existing subscription labels
    """
    subscriber = get_subscriber_client()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    subscription = subscriber.get_subscription(request={"subscription": subscription_path})
    return subscription.labels


def remove_existing_subscription_labels(project_id, subscription_id):
    """
    Remove existing subscription labels
    """
    subscriber = get_subscriber_client()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    # Get the current topic configuration
    subscription = subscriber.get_subscription(request={"subscription": subscription_path})
    # Update the labels
    subscription.labels = {}
    # Update the topic on the server
    update_mask = field_mask_pb2.FieldMask(paths=["labels"])
    subscriber.update_subscription(request={"subscription": subscription,
                                            "update_mask": update_mask})


def update_subscription_labels(project_id, subscription_id, labels):
    """
    Update subscription labels
    """
    subscriber = get_subscriber_client()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)
    # Get the current subscription configuration
    subscription = subscriber.get_subscription(request={"subscription": subscription_path})

    # Update the labels
    subscription.labels.update(labels)

    # Update the subscription on the server
    update_mask = field_mask_pb2.FieldMask(paths=["labels"])
    subscriber.update_subscription(request={"subscription": subscription,
                                            "update_mask": update_mask})


def get_existing_bigtable_labels(project_id, instance_id):
    """
    Get existing BigTable instance labels
    """
    client = get_bigtable_admin_client()
    instance_name = client.instance_path(project_id, instance_id)
    instance = client.get_instance(request={"name": instance_name})
    return instance.labels


def remove_existing_bigtable_labels(project_id, instance_id):
    """
    Remove existing BigTable instance labels
    """
    client = get_bigtable_admin_client()
    instance_name = client.instance_path(project_id, instance_id)

    instance = client.get_instance(request={"name": instance_name})
    instance.labels = {}

    update_mask = field_mask_pb2.FieldMask(paths=["labels"])
    client.partial_update_instance(request={"instance": instance,
                                            "update_mask": update_mask})


def update_bigtable_labels(project_id, instance_id, labels):
    """
    Update BigTable instance labels
    """
    client = get_bigtable_admin_client()
    instance_name = client.instance_path(project_id, instance_id)


    instance = client.get_instance(request={"name": instance_name})
    instance.labels = labels

    update_mask = field_mask_pb2.FieldMask(paths=["labels"])
    client.partial_update_instance(request={"instance": instance,
                                            "update_mask": update_mask})


def get_existing_gce_labels(project_id, zone, instance_id):
    """Gets the labels of a GCE VM.

    Args:
        project_id: The ID of your Google Cloud project.
        zone: The zone where the VM is located.
        instance_name: The name of the VM instance.

    Returns:
        A dictionary containing the VM's labels, or None if an error occurs.
    """
    compute = discovery.build('compute', 'v1')

    instance = compute.instances().get(project=project_id, zone=zone,
                                        instance=instance_id).execute()
    # Get labels, defaulting to an empty dictionary if none exists
    labels = instance.get('labels', {})
    return labels


def update_gce_labels(project_id, zone, instance_id, labels):
    """
    Update GCE instance labels
    """
    client = get_compute_client()
    compute = discovery.build('compute', 'v1')
    instance = compute.instances().get(
        project=project_id, zone=zone, instance=instance_id
        ).execute()

    set_labels_request = compute_v1.InstancesSetLabelsRequest(
        label_fingerprint=instance["labelFingerprint"], labels=labels
        )

    operation = client.set_labels(
        project=project_id,
        zone=zone,
        instance=instance_id,
        instances_set_labels_request_resource=set_labels_request
    )
    # wait for operation
    operation.result(timeout=60)
