"""
This module contains classes for various types of GCP assets.
"""
import utility

class Asset():
    """ Base asset class """
    #pylint: disable=R0913
    def __init__(self, project_id, asset, new_labels, replace_existing, replacement_string):
        self.project_id = project_id
        self.asset_name = asset.name
        self.new_labels = new_labels
        self.replace_existing = replace_existing
        self.replacement_string = replacement_string.replace("$PROJECT_ID", self.project_id)
        self.asset_id = self.get_assetid_from_name()

    def get_assetid_from_name(self):
        """ Get asset id from name base method."""
        return self.asset_name.replace(self.replacement_string, "")

    def update_asset_labels(self):
        """ Base method for updating labels."""
        return

    def print_update_success(self):
        """Prints success message once labels are updated"""
        print(f"{self.asset_id} labels updated to -> {self.new_labels}")


class BigQueryDataset(Asset):
    """ BigQuery Dataset asset class """
    def __init__(self, project_id, asset, labels, replace_existing):
        super().__init__(project_id, asset, labels, replace_existing,
                         "//bigquery.googleapis.com/projects/")
        self.asset_id =  self.asset_id.replace("/datasets/",".")

    def update_asset_labels(self):
        new_labels = {}
        if self.replace_existing:
            # Get current labels
            existing_labels = utility.get_existing_bq_dataset_labels(self.asset_id)
            # To delete a label from a dataset, set its value to None.
            for k, _ in existing_labels.items():
                existing_labels[k] = None

        new_labels = utility.merge_labels(existing_labels, self.new_labels)
        utility.update_bq_dataset_labels(self.asset_id, new_labels)
        self.print_update_success()


class StorageBucket(Asset):
    """ GCS Storage asset class """
    def __init__(self, project_id, asset, labels, replace_existing):
        super().__init__(project_id, asset, labels, replace_existing,
                         "//storage.googleapis.com/")

    def update_asset_labels(self):
        new_labels = {}
        if self.replace_existing:
            new_labels = self.new_labels
        else:
            existing_labels = utility.get_existing_gcs_labels(self.asset_id)
            new_labels =  utility.merge_labels(existing_labels, self.new_labels)

        utility.update_gcs_labels(self.asset_id, new_labels)
        self.print_update_success()


class PubsubTopic(Asset):
    """ PubsubTopic asset class """
    def __init__(self, project_id, asset, labels, replace_existing):
        super().__init__(project_id, asset, labels, replace_existing,
                         "//pubsub.googleapis.com/projects/$PROJECT_ID/topics/")

    def update_asset_labels(self):
        new_labels = {}
        # Get current labels
        existing_labels = utility.get_existing_topic_labels(self.project_id, self.asset_id)
        if self.replace_existing:
            utility.remove_existing_topic_labels(self.project_id, self.asset_id)
            new_labels = self.new_labels
        else:
            new_labels = utility.merge_labels(existing_labels, self.new_labels)

        utility.update_topic_labels(self.project_id, self.asset_id, new_labels)
        self.print_update_success()


class PubsubSubscription(Asset):
    """ PubsubSubscription asset class """
    def __init__(self, project_id, asset, labels, replace_existing):
        super().__init__(project_id, asset, labels, replace_existing,
                         "//pubsub.googleapis.com/projects/$PROJECT_ID/subscriptions/")

    def update_asset_labels(self):
        new_labels = {}
        # Get current labels
        existing_labels = utility.get_existing_subscription_labels(self.project_id, self.asset_id)
        if self.replace_existing:
            utility.remove_existing_subscription_labels(self.project_id, self.asset_id)
            new_labels = self.new_labels
        else:
            new_labels = utility.merge_labels(existing_labels, self.new_labels)

        utility.update_subscription_labels(self.project_id, self.asset_id, new_labels)
        self.print_update_success()


class ComputeInstance(Asset):
    """ ComputeInstance asset class """
    def __init__(self, project_id, asset, labels, replace_existing):
        super().__init__(project_id, asset, labels, replace_existing,
                         "//bigtable.googleapis.com/projects/$PROJECT_ID/instances/")
        self.asset_id, self.zone =  self.get_compute_vm_id_and_zone_from_name()

    def get_compute_vm_id_and_zone_from_name(self):
        """ 
        Get compute VM and zone from name
        Example Name:
        //compute.googleapis.com/projects/dd-proj-1/zones/us-central1-c/instances/instance-1"
        remove everything before zone
        """
        temp_str = self.asset_name.replace(
            f"//compute.googleapis.com/projects/{self.project_id}/zones/", ""
            )
        # us-central1-c/instances/gce-vm-instance-1"
        tokens = str.split(temp_str,"/")
        return tokens[2], tokens[0]


    def update_asset_labels(self):
        new_labels = {}
        # Get current labels
        existing_labels = utility.get_existing_gce_labels(self.project_id, self.zone, self.asset_id)

        if self.replace_existing:
            utility.update_gce_labels(self.project_id, self.zone, self.asset_id, labels={})
            new_labels = self.new_labels
        else:
            new_labels = utility.merge_labels(existing_labels, self.new_labels)

        utility.update_gce_labels(self.project_id, self.zone, self.asset_id, new_labels)
        self.print_update_success()


class BigTableInstance(Asset):
    """ BigTableInstance asset class """
    def __init__(self, project_id, asset, labels, replace_existing):
        super().__init__(project_id, asset, labels, replace_existing,
                         "//bigtable.googleapis.com/projects/$PROJECT_ID/instances/")

    def update_asset_labels(self):
        new_labels = {}
        # Get current labels
        existing_labels = utility.get_existing_bigtable_labels(self.project_id, self.asset_id)
        if self.replace_existing:
            utility.remove_existing_bigtable_labels(self.project_id, self.asset_id)
            new_labels = self.new_labels
        else:
            new_labels = utility.merge_labels(existing_labels, self.new_labels)

        utility.update_bigtable_labels(self.project_id, self.asset_id, new_labels)

        self.print_update_success()
